#
# Copyright (C) 2025 CESNET z.s.p.o.
#
# oarepo-file-pipeline-server is free software; you can redistribute it and/or
# modify it under the terms of the MIT License; see LICENSE file for more
# details.
#
"""PreviewPicture step"""

import io
from typing import AsyncIterator

from PIL import Image


from oarepo_file_pipeline_server.async_to_sync.sync_runner import sync_stream_runner, ResultQueue
from oarepo_file_pipeline_server.pipeline_data.queue_pipeline_data import QueuePipelineData
from oarepo_file_pipeline_server.pipeline_data.url_pipeline_data import UrlPipelineData
from oarepo_file_pipeline_server.pipeline_steps.base import PipelineStep, StepResults
from oarepo_file_pipeline_server.pipeline_data.pipeline_data import PipelineData

class PreviewPicture(PipelineStep):
    """This class is used to preview picture."""

    async def process(self, inputs: AsyncIterator[PipelineData] | None, args: dict) -> StepResults:
        """
        Process picture and yield picture data..

        :param inputs: An asynchronous iterator over `PipelineData` objects.
        :param args: A dictionary of additional arguments (e.g. source_url, max_width, max_height).
        :return: An asynchronous iterator that yields the resulting `QueuePipelineData` object.`.
        :raises ValueError: If no input stream or source URL is provided, or if max_width or max_height is missing.
        :raises Exception: Other exception raised by PIL library.
        """

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
        if height is None or width is None:
            raise ValueError("No max height or no max width provided.")

        results = await sync_stream_runner(image_open, input_stream, height, width)

        item_type, item_value = await results.get()
        if item_type != "file_count":
            results.stop()
            raise ValueError(f"Implementation error, expected 'file_count', got {item_value}")

        async def get_results() -> AsyncIterator[PipelineData]:
            item_type, item_value = await results.get()
            while item_type != 'complete':
                if item_type == 'error':
                    results.stop()
                    raise item_value

                if item_type != 'startfile':
                    results.stop()
                    raise ValueError(f"Implementation error: {item_type}")

                yield QueuePipelineData(results, metadata=item_value)
                item_type, item_value = await results.get()

        return StepResults(1, get_results())

def image_open(input_stream, max_height: int, max_width: int, result_queue: ResultQueue) -> None:
    """Synchronously Open picture, resize if required and sends the extracted picture as chunks."""
    result_queue.put('file_count', 1)

    buffer = io.BytesIO(input_stream.read())
    thumbnailed = False
    # TODO image manipulation ( crop, resize etc...)
    with Image.open(buffer) as image:
        image.load()

        # thumbnail will resize if picture is bigger or keep it if smaller
        if max_width < image.width or max_height < image.height:
            image.thumbnail((max_width, max_height))
            new_buffer = io.BytesIO()
            thumbnailed = True
            image.save(new_buffer, format=image.format)

        result_queue.put('startfile', {
            'file_name' : image.filename,
            'media_type' : f'image/{image.format.lower()}',
            'mode' : f'{image.mode}',
            'width' : image.size[0],
            'height' : image.size[1],
        })
        result_queue.put('chunk',new_buffer.getvalue() if thumbnailed else buffer.getvalue())
        result_queue.put('endfile', b'')


