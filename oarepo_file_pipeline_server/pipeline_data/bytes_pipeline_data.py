#
# Copyright (C) 2025 CESNET z.s.p.o.
#
# oarepo-file-pipeline-server is free software; you can redistribute it and/or
# modify it under the terms of the MIT License; see LICENSE file for more
# details.
#
"""
Class that represents pipeline data with data stored in BytesIO.
"""

import io
import os
from typing import Self

from oarepo_file_pipeline_server.pipeline_data.pipeline_data import PipelineData


class BytesPipelineData(PipelineData):
    """Class representing pipeline data that is stored in a `BytesIO` stream."""
    def __init__(self,metadata: dict, stream: io.BytesIO) -> None:
        """
        Initialize the pipeline data with metadata and an in-memory binary stream.

        :param metadata: A dictionary containing metadata (e.g., file_name, media_type).
        :param stream: An instance of io.BytesIO that holds the binary data.

        Raises:
            ValueError: If the provided stream is not an instance of io.BytesIO.
        """
        if not isinstance(stream, io.BytesIO):
            raise ValueError("Stream must be an instance of io.IOBase.")
        self._stream = stream
        self._stream.seek(0)
        self._metadata = metadata

    async def read(self, n: int = -1) -> bytes:
        """
        Asynchronously read a specific number of bytes from the stream.
        If `n` is -1, reads the entire remaining data from the stream.

        :param n: The number of bytes to read. Defaults to reading the whole stream.
        :return: The bytes read from the stream.
        """
        return self._stream.read(n)

    def __aiter__(self) -> Self:
        """
        Return the iterator for the stream, allowing asynchronous iteration.

        :return: The current instance to be used as an async iterator.
        """
        return self

    async def __anext__(self) -> bytes:
        """
        Asynchronously fetch the next chunk (65000 bytes) of bytes from the stream.
        Raises `StopAsyncIteration` when the entire stream has been read.

        :return: A chunk of bytes read from the stream.
        :raises StopAsyncIteration: If the stream is fully read.
        """
        chunk = await self.read(65000)
        if not chunk:
            raise StopAsyncIteration
        return chunk

    async def seek(self, offset: int, whence:int = os.SEEK_SET) -> int:
        """
        Asynchronously change the stream position based on the given offset and reference point.

        :param offset: The offset to move the stream position.
        :param whence: The reference point for the offset (default is SEEK_SET).
        :return: The new position in the stream.
        """
        return self._stream.seek(offset, whence)

    async def tell(self) -> int:
        """
        Asynchronously get the current position in the stream.

        :return: The current position in the stream.
        """
        return self._stream.tell() # pragma: no cover

    @property
    def metadata(self) -> dict:
        """
        Property that holds the metadata associated with the pipeline data.

        :return: The metadata dictionary, which may include keys like file_name, media_type.
        """
        return self._metadata

    @metadata.setter
    def metadata(self, value: dict) -> None:
        """
        Setter for updating the metadata of the pipeline data.

        :param value: A dictionary of metadata to update the current metadata.
        """
        self._metadata.update(value) # pragma: no cover
