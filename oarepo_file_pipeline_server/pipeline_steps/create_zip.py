#
# Copyright (C) 2025 CESNET z.s.p.o.
#
# oarepo-file-pipeline-server is free software; you can redistribute it and/or
# modify it under the terms of the MIT License; see LICENSE file for more
# details.
#
"""CreateZip step"""
import io
from typing import AsyncIterator, Self

from oarepo_file_pipeline_server.pipeline_steps.base import PipelineStep
from oarepo_file_pipeline_server.pipeline_data.pipeline_data import PipelineData
from stat import S_IFREG
from stream_zip import ZIP_64, async_stream_zip


# https://stream-zip.docs.trade.gov.uk/async-interface/
# https://github.com/uktrade/stream-zip/blob/590f66c6aeff36e11f8f0b40a78f47e9c87994dc/stream_zip/__init__.py#L793

# Another possible option is https://github.com/sandes/zipfly
class ZipPipelineData(PipelineData):
    """
    This class is used to wrap multiple input data streams and compress them into a ZIP format.
    It allows asynchronous reading from the resulting ZIP stream.
    """
    def __init__(self, input_streams: AsyncIterator[PipelineData], metadata: dict):
        """
        Initialize ZipPipelineData.

        :param input_streams: The input data streams to be compressed.
        :param metadata: Metadata related to the ZIP file (e.g., file name, media type).
        """
        self.input_streams = input_streams
        self.zipped_file_iterator = aiter(async_stream_zip(self.async_member_files(self.input_streams)))
        self.current_chunk = None
        self.current_chunk_pos = 0
        self._metadata = metadata

    async def read(self, n:int=-1) -> bytes:
        """
        Read the next `n` bytes from the zipped data stream asynchronously.

        If `n` is -1, it reads the remaining data. Otherwise, it reads up to `n` bytes.

        :param n: The number of bytes to read. If -1, read all remaining data.
        :return: A byte string containing the read data.
        """
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
    def metadata(self) -> dict:
        """
       Property holding metadata like file_name, media_type, etc.

       :return: The metadata dictionary.
       """
        return self._metadata

    def __aiter__(self) -> Self:
        """
        Return the iterator itself to allow asynchronous iteration over the zipped content.

        :return: The ZipPipelineData instance.
        """
        return self

    async def __anext__(self) -> bytes:
        """
        Retrieve the next chunk of zipped data.

        :return: A chunk of zipped data.
        """
        self.current_chunk = await anext(self.zipped_file_iterator)
        self.current_chunk_pos = 0

        return self.current_chunk

    async def get_async_data(self, input_stream: PipelineData) -> AsyncIterator[bytes]:
        """
        Asynchronously stream data from the input stream.

        :param input_stream: The input data to be streamed.
        :return: An asynchronous iterator yielding bytes from the input stream.
        """
        async for chunk in input_stream:
            yield chunk

    async def async_member_files(self, input_streams: AsyncIterator[PipelineData]) -> AsyncIterator:
        """
        Prepare the input data streams as members for the zip file (needed by stream-zip library).

        :param input_streams: An asynchronous iterator yielding input data to be zipped.
        :return: An asynchronous iterator yielding file metadata and data for each input stream.
        """
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
    """Pipeline step to create a ZIP file from multiple input data streams."""
    async def process(self, inputs: AsyncIterator[PipelineData] | None, args: dict) -> AsyncIterator[PipelineData] | None:
        """
        Create a zip file from the inputs provided by the pipeline.

        :param inputs: An asynchronous iterator over `PipelineData` objects to be zipped.
        :param args: A dictionary of additional arguments, not used here.
        :return: An asynchronous iterator that yields the resulting `ZipPipelineData`.
        :raises ValueError: If no input data is provided.
        """
        if not inputs:
            raise ValueError("No input data provided to CreateZip step.")
        assert not isinstance(inputs, PipelineData)

        yield ZipPipelineData(inputs, metadata={
            'file_name' : "created.zip",
            'media_type': 'application/zip',
            'headers': {'Content-Disposition': f'attachment; filename="created.zip"',}}
        )
