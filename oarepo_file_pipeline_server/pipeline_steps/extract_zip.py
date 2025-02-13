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
from pathlib import Path
from typing import AsyncIterator, override

from oarepo_file_pipeline_server.async_to_sync.sync_runner import ResultQueue, sync_stream_runner
from oarepo_file_pipeline_server.pipeline_data.queue_pipeline_data import QueuePipelineData
from oarepo_file_pipeline_server.pipeline_data.url_pipeline_data import UrlPipelineData
from oarepo_file_pipeline_server.pipeline_steps.base import PipelineStep, StepResults, UNKNOWN_FILE_COUNT
from oarepo_file_pipeline_server.pipeline_data.pipeline_data import PipelineData

class ExtractZip(PipelineStep):
    """This class is used to extract zip files from a specific folder."""

    @override
    async def process(self, inputs: AsyncIterator[PipelineData] | None, args: dict) -> StepResults:
        """
        Extract zip files from a specific folder and yield individual extracted files.

        :param inputs: An asynchronous iterator over `PipelineData` objects to be unzipped.
        :param args: A dictionary of additional arguments (e.g. source_url, directory_name).
        :return: An asynchronous iterator that yields the resulting `QueuePipelineData` objects.`.
        :raises ValueError: If no input stream or source URL is provided, or if directory name is missing.
        :raises Exception: Other exception raised by zipfile library.
        """
        if not inputs and not args:
            raise ValueError("No input or arguments were provided to ExtractZip step.")
        if inputs:
            assert not isinstance(inputs, PipelineData)

            input_stream = await anext(inputs)
        elif args and "source_url" in args:
            from oarepo_file_pipeline_server.utils import http_session
            input_stream = UrlPipelineData(args["source_url"], http_session)
        else:
            raise ValueError("No input nor source_url were provided.")

        directory_or_file_name = args.get("directory_or_file_name")
        if not directory_or_file_name:
            raise ValueError("Directory or file name needed to extract is not specified in arguments.")

        results = await sync_stream_runner(extract_zip, input_stream, directory_or_file_name)

        item_type, file_count = await results.get()
        if item_type != "file_count":
            results.stop()
            raise ValueError(f"Implementation error, expected 'file_count', got {file_count}.")

        async def get_results() -> AsyncIterator[PipelineData]:
            item_type, item_value = await results.get()
            while item_type != 'complete':
                if item_type == 'error':
                    results.stop()
                    raise item_value

                if item_type != 'startfile' :
                    results.stop()
                    raise ValueError(f"Implementation error: {item_type}")

                yield QueuePipelineData(results.queue, metadata=item_value)
                item_type, item_value = await results.get()

        return StepResults(file_count, get_results())

def extract_zip(input_stream, directory_or_file_name: str, result_queue: ResultQueue) -> None:
    """Synchronously Extracts a specific directory from the ZIP file and sends the extracted files as chunks."""

    if not zipfile.is_zipfile(input_stream):
        raise ValueError("Input stream is not a valid ZIP file.")


    directory_or_file_name = directory_or_file_name.strip("/")

    with zipfile.ZipFile(input_stream, 'r') as zip_file:
        for info in zip_file.infolist():
            if info.filename == directory_or_file_name and not info.is_dir():
                # just file
                result_queue.put('file_count',1)
                extract_file(zip_file, str(Path(info.filename).parent), info.filename, result_queue)
                break
            elif info.is_dir() and info.filename.strip("/") == directory_or_file_name:
                name_list = [name for name in zip_file.namelist() if name.startswith(info.filename) and not name.endswith("/")]
                result_queue.put('file_count',UNKNOWN_FILE_COUNT)
                for name in name_list:
                    extract_file(zip_file, info.filename, name, result_queue)



def extract_file(zip_file: zipfile.ZipFile, base_name:str, file_name: str, result_queue: ResultQueue) -> None:
    with zip_file.open(file_name, 'r') as extracted_file:
        relative_path_name = os.path.relpath(file_name, base_name)
        mime_type, _ = mimetypes.guess_type(relative_path_name)

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
