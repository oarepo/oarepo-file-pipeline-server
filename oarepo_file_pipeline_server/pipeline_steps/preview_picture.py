from io import BytesIO
from typing import AsyncIterator

from PIL import Image


from oarepo_file_pipeline_server.async_to_sync.sync_runner import sync_stream_runner, read_result, ResultQueue
from oarepo_file_pipeline_server.pipeline_data.bytes_pipeline_data import BytesPipelineData
from oarepo_file_pipeline_server.pipeline_data.queue_pipeline_data import QueuePipelineData
from oarepo_file_pipeline_server.pipeline_data.url_pipeline_data import UrlPipelineData
from oarepo_file_pipeline_server.pipeline_steps.base import PipelineStep
from oarepo_file_pipeline_server.pipeline_data.pipeline_data import PipelineData

class PreviewPicture(PipelineStep):
    async def process(self, inputs: AsyncIterator[PipelineData] | None, args: dict) -> AsyncIterator[PipelineData] | None:
        if inputs is None and not args:
            raise ValueError("No input or arguments were provided to PreviewPicture step.")
        if inputs:
            assert not isinstance(inputs, PipelineData)

            input_stream = await anext(inputs)
        elif args and "source_url" in args:
            from oarepo_file_pipeline_server.utils import http_session
            input_stream = UrlPipelineData(args["source_url"], http_session)
        else:
            raise ValueError("No input provided.")

        height = args.get("max_height")
        width = args.get("max_width")
        results = await sync_stream_runner(image_open, input_stream, height, width)
        item_type, item_value = await results.get()
        print("Returned from image_open")

        while item_type != 'complete':
            if item_type == 'error':
                raise item_value

            if item_type != 'startfile':
                raise ValueError(f"Implementation error: {item_type}")

            yield QueuePipelineData(results, metadata=item_value)
            item_type, item_value = await results.get()


def image_open(input_stream, max_height, max_width, result_queue: ResultQueue):
    if max_height is None and max_width is None:
        raise ValueError("No max height and no max width provided.")

    buffer = BytesIO()

    # TODO image manipulation ( crop, resize etc...)
    with Image.open(input_stream) as image:
        print("Opened image")
        image.load()
        # thumbnail will resize if picture is bigger or keep it if smaller
        image.thumbnail((max_width, max_height))

        # TODO quality arg is for JPEG only, do for other formats too
        image.save(buffer, format=image.format, quality='keep')

        result_queue.put('startfile', {
            'file_name' : image.filename,
            'media_type' : f'image/{image.format.lower()}',
            'mode' : f'{image.mode}',
            'width' : image.width,
            'height' : image.height,
        })
        result_queue.put('chunk',buffer.getvalue())
        result_queue.put('endfile', b'')



