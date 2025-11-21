#
# Copyright (C) 2025 CESNET z.s.p.o.
#
# oarepo-file-pipeline-server is free software; you can redistribute it and/or
# modify it under the terms of the MIT License; see LICENSE file for more
# details.
#
"""Protocol defining the data structure for pipeline operations.

This protocol ensures that any class implementing it will support the essential
methods for handling data in a synchronous pipeline. The data structure must
at least support reading, iterating, and providing chunks of data.
"""

from __future__ import annotations

from typing import Protocol, Self, runtime_checkable


@runtime_checkable
class PipelineData(Protocol):
    """A protocol that defines the interface for pipeline data handling.

    Any class that implements this protocol must be capable of reading data,
    and iterating over it in chunks.
    """

    def read(self, n: int = -1) -> bytes:
        """Read a specific number of bytes from the data stream.

        :param n: The number of bytes to read. Defaults to reading the entire stream if `n` is -1.
        :return: The bytes read from the stream.
        """
        ...

    def __iter__(self) -> Self:
        """Prepare the object for iteration.

        Returns the object itself, making it an iterable object that can be used
        in a for loop.

        :return: The current object, which is the iterable.
        """
        ...

    def __next__(self) -> bytes:
        """Retrieve the next chunk of data from the pipeline.

        This method is used in iteration (e.g., within a for loop).
        Raises `StopIteration` when all data has been processed.

        :return: The next chunk of data as bytes.
        :raises StopIteration: When there is no more data to iterate over.
        """
        ...

    @property
    def metadata(self) -> dict:
        """Retrieve metadata associated with the pipeline data.

        This property provides access to any metadata information that may be
        relevant to the data being processed in the pipeline.

        :return: A dictionary containing metadata information.
        """
        ...
