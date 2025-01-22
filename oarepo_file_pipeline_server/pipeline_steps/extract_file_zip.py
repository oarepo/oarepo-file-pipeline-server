#
# Copyright (C) 2025 CESNET z.s.p.o.
#
# oarepo-file-pipeline-server is free software; you can redistribute it and/or
# modify it under the terms of the MIT License; see LICENSE file for more
# details.
#
"""ExtractFileZip step."""

import mimetypes
import os
import zipfile
from typing import AsyncIterator

from oarepo_file_pipeline_server.async_to_sync.sync_runner import sync_stream_runner, ResultQueue
from oarepo_file_pipeline_server.pipeline_data.queue_pipeline_data import QueuePipelineData
from oarepo_file_pipeline_server.pipeline_data.url_pipeline_data import UrlPipelineData
from oarepo_file_pipeline_server.pipeline_steps.base import PipelineStep
from oarepo_file_pipeline_server.pipeline_data.pipeline_data import PipelineData


class ExtractFileZip(PipelineStep):
    """This class is used to extract zip file."""

    async def process(self, inputs: AsyncIterator[PipelineData] | None, args: dict) -> AsyncIterator[PipelineData] | None:
        """
        Extract zip file and yield extracted file.

        :param inputs: An asynchronous iterator over `PipelineData` objects to be unzipped.
        :param args: A dictionary of additional arguments (e.g. source_url, file_name).
        :return: An asynchronous iterator that yields the resulting `QueuePipelineData` objects.`.
        :raises ValueError: If no input stream or source URL is provided, or if file name is missing.
        :raises Exception: Other exception raised by zipfile library.
        """
        if not inputs and not args:
            raise ValueError("No input or arguments were provided to ExtractFile step.")
        if inputs:
            assert not isinstance(inputs, PipelineData)

            input_stream = await anext(inputs)
        elif args and "source_url" in args:
            from oarepo_file_pipeline_server.utils import http_session
            input_stream = UrlPipelineData(args["source_url"], http_session)
        else:
            raise ValueError("No input provided.")

        file_name = args.get("file_name")
        results = await sync_stream_runner(zip_open_file, input_stream, file_name)

        item_type, item_value = await results.get()
        while item_type != 'complete':
            if item_type == 'error':
                raise item_value

            if item_type != 'startfile' :
                raise ValueError(f"Implementation error: {item_type}")

            yield QueuePipelineData(results, metadata=item_value)
            item_type, item_value = await results.get()


def zip_open_file(input_stream, file_name: str, result_queue: ResultQueue) -> None:
    """Synchronously Extracts a specific file from the ZIP file and sends the extracted file as chunks."""
    if not file_name:
        raise ValueError("No file name to extract was provided.")

    if not zipfile.is_zipfile(input_stream):
        raise ValueError("Input stream is not a valid ZIP file.")

    filename = os.path.basename(file_name)
    mime_type, _ = mimetypes.guess_type(filename)

    with zipfile.ZipFile(input_stream, 'r') as zip_file:
        if file_name not in zip_file.namelist():
            raise ValueError(f"File '{file_name}' not found in the ZIP archive.")

        with zip_file.open(file_name, 'r') as extracted_file:
            result_queue.put('startfile', {
                'file_name': file_name,
                'media_type': mime_type if mime_type else "application/octet-stream",
            })


            while True:
                chunk = extracted_file.read(1024 * 1024) # 1 MB
                if not chunk:
                    break

                result_queue.put('chunk', chunk)

            result_queue.put('endfile', b'EOF')
