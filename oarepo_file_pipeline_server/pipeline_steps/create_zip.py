import io
from typing import AsyncIterator

from oarepo_file_pipeline_server.pipeline_steps.base import PipelineStep
from oarepo_file_pipeline_server.pipeline_data.pipeline_data import PipelineData
from stat import S_IFREG
from stream_zip import ZIP_64, async_stream_zip


# https://stream-zip.docs.trade.gov.uk/async-interface/
# https://github.com/uktrade/stream-zip/blob/590f66c6aeff36e11f8f0b40a78f47e9c87994dc/stream_zip/__init__.py#L793

# Another possible option is https://github.com/sandes/zipfly
class ZipPipelineData(PipelineData):
    def __init__(self, input_streams, metadata: dict):
        self.input_streams = input_streams
        self.zipped_file_iterator = aiter(async_stream_zip(self.async_member_files(self.input_streams)))
        self.current_chunk = None
        self.current_chunk_pos = 0
        self._metadata = metadata

    async def read(self, n:int=-1) -> bytes:
        if not self.current_chunk or self.current_chunk_pos >= len(self.current_chunk):
            try:
                await anext(self)
            except StopAsyncIteration:
                return b''

        if n == -1:
            buffer = io.BytesIO()
            while item := await self.read(65000):
                buffer.write(item)

            return buffer.getvalue()

        ret = self.current_chunk[self.current_chunk_pos:self.current_chunk_pos+n]
        self.current_chunk_pos += len(ret)

        remaining = n - len(ret)
        if remaining > 0:
            ret += await self.read(remaining)

        return ret

    @property
    def metadata(self):
        return self._metadata

    def __aiter__(self):
        return self

    async def __anext__(self):
        self.current_chunk = await anext(self.zipped_file_iterator)
        self.current_chunk_pos = 0

        return self.current_chunk

    async def get_async_data(self, input_stream) -> bytes:
        async for chunk in input_stream:
            yield chunk

    async def async_member_files(self, input_streams):
        from datetime import datetime
        file_id = 0
        async for current_file in input_streams:
            yield (
                current_file.metadata.get('file_name', f"file_{file_id}"),
                datetime.now(),
                S_IFREG | 0o600,
                ZIP_64,
                self.get_async_data(current_file)
            )
            file_id += 1

class CreateZip(PipelineStep):
    async def process(self, inputs: AsyncIterator[PipelineData] | None, args: dict) -> AsyncIterator[PipelineData] | None:
        """
        Create zip file from inputs list.
        """
        if not inputs:
            raise ValueError("No input data provided to CreateZip step.")
        assert not isinstance(inputs, PipelineData)

        yield ZipPipelineData(inputs, metadata={
            'file_name' : "created.zip",
            'media_type': 'application/zip',
            'headers': {'Content-Disposition': f'attachment; filename="created.zip"',}}
        )
