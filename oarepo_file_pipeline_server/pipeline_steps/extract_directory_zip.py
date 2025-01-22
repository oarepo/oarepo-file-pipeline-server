#
# Copyright (C) 2025 CESNET z.s.p.o.
#
# oarepo-file-pipeline-server is free software; you can redistribute it and/or
# modify it under the terms of the MIT License; see LICENSE file for more
# details.
#
"""ExtractDirectoryZip step."""

import mimetypes
import os
import zipfile
from typing import AsyncIterator

from oarepo_file_pipeline_server.async_to_sync.sync_runner import ResultQueue, sync_stream_runner
from oarepo_file_pipeline_server.pipeline_data.queue_pipeline_data import QueuePipelineData
from oarepo_file_pipeline_server.pipeline_data.url_pipeline_data import UrlPipelineData
from oarepo_file_pipeline_server.pipeline_steps.base import PipelineStep
from oarepo_file_pipeline_server.pipeline_data.pipeline_data import PipelineData

class ExtractDirectoryZip(PipelineStep):
    """This class is used to extract zip files from a specific folder."""

    produces_multiple_outputs = True

    async def process(self, inputs: AsyncIterator[PipelineData] | None, args: dict) -> AsyncIterator[PipelineData] | None:
        """
        Extract zip files from a specific folder and yield individual extracted files.

        :param inputs: An asynchronous iterator over `PipelineData` objects to be unzipped.
        :param args: A dictionary of additional arguments (e.g. source_url, directory_name).
        :return: An asynchronous iterator that yields the resulting `QueuePipelineData` objects.`.
        :raises ValueError: If no input stream or source URL is provided, or if directory name is missing.
        :raises Exception: Other exception raised by zipfile library.
        """
        if not inputs and not args:
            raise ValueError("No input or arguments were provided to PreviewZip step.")
        if inputs:
            assert not isinstance(inputs, PipelineData)

            input_stream = await anext(inputs)
        elif args and "source_url" in args:
            from oarepo_file_pipeline_server.utils import http_session
            input_stream = UrlPipelineData(args["source_url"], http_session)
            print("Read file from s3")
        else:
            raise ValueError("No input nor source_url were provided.")

        directory_name = args.get("directory_name")
        results = await sync_stream_runner(extract_directory_zip, input_stream, directory_name)

        item_type, item_value = await results.get()
        while item_type != 'complete':
            if item_type == 'error':
                raise item_value

            if item_type != 'startfile' :
                raise ValueError(f"Implementation error: {item_type}")

            yield QueuePipelineData(results, metadata=item_value)
            item_type, item_value = await results.get()


def extract_directory_zip(input_stream, directory_name: str, result_queue: ResultQueue) -> None:
    """Synchronously Extracts a specific directory from the ZIP file and sends the extracted files as chunks."""
    if not zipfile.is_zipfile(input_stream):
        raise ValueError("Input stream is not a valid ZIP file.")

    if not directory_name:
        raise ValueError("Directory name to extract is not specified in arguments.")
    print(f'need to extract {directory_name=}')

    # TODO check whether it is root of the zip to extract all files
    with zipfile.ZipFile(input_stream, 'r') as zip_file:
        # Normalize directory name to ensure it ends with 1 '/'
        # in case user made a typo with 2 backslashes instead of 1 etc.
        directory_name = directory_name.rstrip("/") + "/"

        print(f"Extracting {directory_name} folder...")
        for file_name in zip_file.namelist():
            if file_name.startswith(directory_name) and not file_name.endswith("/"):  # Exclude subdirectories
                with zip_file.open(file_name, 'r') as extracted_file:
                    relative_path_name = os.path.relpath(file_name, directory_name)
                    mime_type, _ = mimetypes.guess_type(relative_path_name)
                    print(f'Extracting {relative_path_name}...')

                    result_queue.put('startfile', {
                        'file_name': relative_path_name,
                        'media_type': mime_type if mime_type else "application/octet-stream",
                    })

                    while True:
                        chunk = extracted_file.read(1024 * 1024)
                        if not chunk:
                            break

                        result_queue.put('chunk', chunk)

                    result_queue.put('endfile', b'EOF')
