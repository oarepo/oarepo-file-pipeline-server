#
# Copyright (C) 2025 CESNET z.s.p.o.
#
# oarepo-file-pipeline-server is free software; you can redistribute it and/or
# modify it under the terms of the MIT License; see LICENSE file for more
# details.
#
"""
Class that represents pipeline data with data stored in asyncio.Queue in chunks.
"""
import asyncio
import io

from oarepo_file_pipeline_server.pipeline_data.pipeline_data import PipelineData
from typing import Self

class QueuePipelineData(PipelineData):
    def __init__(self, queue: asyncio.Queue, metadata:dict) -> None:
        """Initialize the QueuePipelineData with the given queue and metadata."""
        self.queue = queue # queue with tuples ('chunk', bytes),('startfile', metadata), ('endfile', bytes)
        self.current_chunk: bytes | None = None
        self.current_chunk_index = 0
        self.current_chunk_size = 0
        self._metadata = metadata
        self.end_of_file = False

    def __aiter__(self) -> Self:
        """
        Prepare the object for asynchronous iteration.

        This method returns the object itself, making it iterable in an asynchronous for loop.
        """
        return self

    async def __anext__(self) -> bytes:
        """
        Asynchronously retrieve the next chunk of data from the queue.

        This method is used for asynchronous iteration (e.g., within an `async for` loop).
        If no more data is available, it raises a `StopAsyncIteration` exception.

        :return: A chunk of data (65000 bytes).
        :raises StopAsyncIteration: If there is no more data to read.
        """
        chunk = await self.read(65000)
        if not chunk:
            raise StopAsyncIteration
        return chunk

    async def read(self, n: int = -1) -> bytes:
        """
        Asynchronously read a specific number of bytes from the queue.

        If `n == -1`, it reads all data until the end of the file. It also handles reading
        in chunks and processing the current chunk of data.

        :param n: The number of bytes to read. Defaults to reading the whole stream if `n` is -1.
        :return: A chunk of data as bytes.
        :raises ValueError: If an unsupported chunk type is encountered.
        :raises Exception: If there is an error while processing the data chunk.
        """
        if self.end_of_file: # pragma: no cover
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
            raise item_value # pragma: no cover

        if item_type == 'endfile':
            self.end_of_file = True
            return b''
        elif item_type == 'chunk':
            self.current_chunk = item_value
            self.current_chunk_index = 0
            self.current_chunk_size = len(item_value)
            return await self.read(n)
        else:
            raise ValueError(f'Unsupported chunk type: {item_type}') # pragma: no cover

    @property
    def metadata(self) -> dict:
        """
        Property holding metadata like file_name, media_type, etc.

        :return: The metadata dictionary.
        """
        return self._metadata # pragma: no cover

    @metadata.setter
    def metadata(self, value: dict) -> None:
        """
        Setter for updating metadata.

        :param value: A dictionary with new metadata values.
        """
        self._metadata.update(value) # pragma: no cover


