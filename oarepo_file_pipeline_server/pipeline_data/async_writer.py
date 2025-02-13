#
# Copyright (C) 2025 CESNET z.s.p.o.
#
# oarepo-file-pipeline-server is free software; you can redistribute it and/or
# modify it under the terms of the MIT License; see LICENSE file for more
# details.
#
"""Basic async write wrapper for QueuePipelineData."""
from typing import Self

from oarepo_file_pipeline_server.async_to_sync.sync_runner import ResultQueue
from oarepo_file_pipeline_server.pipeline_data.pipeline_data import PipelineData


class AsyncWriter(PipelineData):
    """Writer class around QueuePipelineData."""
    def __init__(self, file_count:int, metadata: dict, queue: ResultQueue) -> None:
        """Initialize AsyncWriter."""
        self.metadata = metadata
        self.queue = queue
        self.queue.put('file_count', file_count)
        self.queue.put('startfile', metadata)

    def write(self, bytes_array) -> None:
        """Write bytes to queue."""
        self.queue.put('chunk', bytes_array)

    def __enter__(self) -> Self:
        """Context manager enter."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit."""
        self.close()

    def close(self) -> None:
        """Place endfile to the queue after closing the stream"""
        self.queue.put('endfile', b'')