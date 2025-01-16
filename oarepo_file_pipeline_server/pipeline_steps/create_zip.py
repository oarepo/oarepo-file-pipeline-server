import asyncio
from typing import AsyncIterator


from oarepo_file_pipeline_server.pipeline_data.queue_pipeline_data import QueuePipelineData
from oarepo_file_pipeline_server.pipeline_steps.base import PipelineStep
from oarepo_file_pipeline_server.pipeline_data.pipeline_data import PipelineData
from stat import S_IFREG
from stream_zip import ZIP_64, async_stream_zip

class CreateZip(PipelineStep):
    async def process(self, inputs: AsyncIterator[PipelineData] | None, args: dict) -> AsyncIterator[PipelineData] | None:
        """
        Create zip file from inputs list.
        """
        if not inputs:
            raise ValueError("No input data provided to CreateZip step.")
        assert not isinstance(inputs, PipelineData)

        # create queue and QueuePipelineData
        queue = asyncio.Queue()
        output = QueuePipelineData(metadata={
            'file_name' : "created.zip",
            'media_type': 'application/zip',
            'headers': {'Content-Disposition': f'attachment; filename="created.zip"'}
        }, queue=queue)

        # create task for filling up output queue with chunks of data
        asyncio.create_task(fill_queue(queue, inputs))
        yield output


async def fill_queue(queue, inputs):
    try:
        async for chunk in async_stream_zip(async_member_files(inputs)):
            # print(f'placing chunk, size of chunk: {len(chunk)}')
            await queue.put(('chunk', chunk))
    finally:
        await queue.put(('endfile', b''))

# https://stream-zip.docs.trade.gov.uk/async-interface/
# https://github.com/uktrade/stream-zip/blob/590f66c6aeff36e11f8f0b40a78f47e9c87994dc/stream_zip/__init__.py#L793

# Another possible option is https://github.com/sandes/zipfly
async def get_async_data(input_stream):
    async for chunk in input_stream:
        yield chunk

async def async_member_files(input_stream):
    from datetime import datetime
    file_id = 0
    async for current_file in input_stream:
        yield (
            current_file.metadata.get('file_name', f"file_{file_id}"),
            datetime.now(),
            S_IFREG | 0o600,
            ZIP_64,
            get_async_data(current_file)
        )
        file_id += 1
