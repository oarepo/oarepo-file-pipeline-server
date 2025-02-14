#
# Copyright (C) 2025 CESNET z.s.p.o.
#
# oarepo-file-pipeline-server is free software; you can redistribute it and/or
# modify it under the terms of the MIT License; see LICENSE file for more
# details.
#
"""Protocol defining the data structure for pipeline operations.

This protocol ensures that any class implementing it will support the essential
methods for handling data in an asynchronous pipeline. The data structure must
at least support reading, iterating, and providing chunks of data asynchronously.
"""

from typing import Protocol, runtime_checkable, Self


@runtime_checkable
class PipelineData(Protocol):
    """
    A protocol that defines the interface for pipeline data handling.

    Any class that implements this protocol must be capable of reading data,
    and asynchronously iterating over it in chunks.
    """
    async def read(self, n:int=-1) -> bytes:
        """
        Asynchronously read a specific number of bytes from the data stream.

        :param n: The number of bytes to read. Defaults to reading the entire stream if `n` is -1.
        :return: The bytes read from the stream.
        """
        ... # pragma: no cover

    def __aiter__(self) -> Self:
        """
        Prepare the object for asynchronous iteration.

        Returns the object itself, making it an iterable object that can be used
        in an asynchronous for loop.

        :return: The current object, which is the iterable.
        """
        ... # pragma: no cover

    async def __anext__(self) -> bytes:
        """
        Asynchronously retrieve the next chunk of data from the pipeline.

        This method is used in an asynchronous iteration (e.g., within an async for loop).
        Raises `StopAsyncIteration` when all data has been processed.

        :return: The next chunk of data as bytes.
        :raises StopAsyncIteration: When there is no more data to iterate over.
        """
        ... # pragma: no cover