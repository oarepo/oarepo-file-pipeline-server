import io

from oarepo_file_pipeline_server.pipeline_data.pipeline_data import PipelineData


class BytesPipelineData(PipelineData):
    def __init__(self,metadata: dict,  stream: io.BytesIO) -> None:
        """
        Initialize with an input stream.

        :param stream: An IOBase-like object for input.
        """
        if not isinstance(stream, io.BytesIO):
            raise ValueError("stream must be an instance of io.IOBase.")
        self._stream = stream
        self._metadata = metadata

    async def read(self, n: int = -1):
        #print(f"Bytes pipeline data read, n = {n}")
        return self._stream.read(n)

    def __aiter__(self):
        return self

    async def __anext__(self):
        chunk = await self.read(65000)
        if not chunk:
            raise StopAsyncIteration
        return chunk

    async def seek(self, offset, whence):
        print('seeking')
        return self._stream.seek(offset, whence)

    async def tell(self):
        return self._stream.tell()

    def get_stream(self) -> io.IOBase:
        """
        Get the input stream.
        :return: The stream object.
        """
        return self._stream

    @property
    def metadata(self) -> dict:
        """Property holding metadata like file_name, media_type, etc."""
        return self._metadata

    @metadata.setter
    def metadata(self, value: dict) -> None:
        """Setter for metadata."""
        self._metadata.update(value)
