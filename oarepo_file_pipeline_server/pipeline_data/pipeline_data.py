import io

class PipelineData:
    def __init__(self, stream: io.IOBase,) -> None:
        """
        Initialize with an input stream.

        :param stream: An IOBase-like object for input.
        """
        if not isinstance(stream, io.IOBase):
            raise ValueError("stream must be an instance of io.IOBase.")
        self._stream = stream
        self._metadata = dict()


    def get_stream(self) -> io.IOBase:
        """
        Get the input stream.
        :return: The stream object.
        """
        self._stream.seek(0)
        return self._stream

    @property
    def metadata(self) -> dict:
        """Property holding metadata like file_name, media_type, etc."""
        return self._metadata

    @metadata.setter
    def metadata(self, value: dict) -> None:
        """Setter for metadata."""
        self._metadata.update(value)
