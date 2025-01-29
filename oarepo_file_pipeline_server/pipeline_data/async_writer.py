import asyncio

from oarepo_file_pipeline_server.async_to_sync.sync_runner import ResultQueue
from oarepo_file_pipeline_server.pipeline_data.pipeline_data import PipelineData


class AsyncWriter(PipelineData):
    def __init__(self, metadata: dict, queue: ResultQueue):
        self.metadata = metadata
        self.queue = queue
        self.queue.put('startfile', metadata)

    def write(self, bytes_array):
        self.queue.put('chunk', bytes_array)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        self.queue.put('endfile', b'')