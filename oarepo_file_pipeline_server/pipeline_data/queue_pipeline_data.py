import asyncio
import io

from oarepo_file_pipeline_server.pipeline_data.pipeline_data import PipelineData


class QueuePipelineData(PipelineData):
    def __init__(self, queue: asyncio.Queue, metadata:dict):
        self.queue = queue
        self.current_chunk: bytes | None = None
        self.current_chunk_index = 0
        self.current_chunk_size = 0
        self._metadata = metadata
        self.end_of_file = False

    def __aiter__(self):
        return self

    async def __anext__(self):
        chunk = await self.read(65000)
        if not chunk:
            raise StopAsyncIteration
        return chunk

    async def read(self, n: int = -1) -> bytes:
        if self.end_of_file:
            return b''
        if n == -1:
            buffer = io.BytesIO()

            while item := await self.read(65000):
                buffer.write(item)

            return buffer.getvalue()
        assert n > 0

        if self.current_chunk is not None:
            if self.current_chunk_index < self.current_chunk_size:
                ret = self.current_chunk[self.current_chunk_index : self.current_chunk_index + n]
                self.current_chunk_index += n
                return ret

        item_type, item_value = await self.queue.get()
        if item_type == 'error':
            raise item_value

        if item_type == 'endfile':
            self.end_of_file = True
            return b''
        elif item_type == 'chunk':
            self.current_chunk = item_value
            self.current_chunk_index = 0
            self.current_chunk_size = len(item_value)
            return await self.read(n)
        else:
            raise ValueError(f'Unsupported chunk type: {item_type}')

    @property
    def metadata(self) -> dict:
        """Property holding metadata like file_name, media_type, etc."""
        return self._metadata

    @metadata.setter
    def metadata(self, value: dict) -> None:
        """Setter for metadata."""
        self._metadata.update(value)


