#
# Copyright (C) 2025 CESNET z.s.p.o.
#
# oarepo-file-pipeline-server is free software; you can redistribute it and/or
# modify it under the terms of the MIT License; see LICENSE file for more
# details.
#
"""PreviewZip step"""

import io
import json
import mimetypes
import zipfile
from datetime import datetime
from typing import AsyncIterator

from oarepo_file_pipeline_server.async_to_sync.sync_runner import sync_stream_runner, ResultQueue, read_result
from oarepo_file_pipeline_server.pipeline_data.bytes_pipeline_data import BytesPipelineData
from oarepo_file_pipeline_server.pipeline_data.url_pipeline_data import UrlPipelineData
from oarepo_file_pipeline_server.pipeline_steps.base import PipelineStep, StepResults
from oarepo_file_pipeline_server.pipeline_data.pipeline_data import PipelineData

class PreviewZip(PipelineStep):
    """This class is used to preview zip."""

    async def process(self, inputs: AsyncIterator[PipelineData] | None, args: dict) -> StepResults:
        """
        Extract zip file and yield extracted file.

        :param inputs: An asynchronous iterator over `PipelineData` objects.
        :param args: A dictionary of additional arguments (e.g. source_url).
        :return: An asynchronous iterator that yields the resulting `BytesPipelineData`.
        :raises ValueError: If no input stream or source URL is provided, or if input is not valid zip file.
        :raises Exception: Other exception raised by zipfile library.
        """

        if inputs is None and not args:
            raise ValueError("No input or arguments were provided to PreviewZip step.")
        if inputs:
            assert not isinstance(inputs, PipelineData)
            input_stream = await anext(inputs)
        elif args and "source_url" in args:
            from oarepo_file_pipeline_server.utils import http_session
            input_stream = UrlPipelineData(args["source_url"], http_session)
        else:
            raise ValueError("No input or source_link were provided.")

        results = await sync_stream_runner(zip_namelist, input_stream)
        namelist = await read_result(results.queue)

        async def get_results() -> AsyncIterator[PipelineData]:
            yield BytesPipelineData({'media_type': 'application/json'}, io.BytesIO(namelist.encode('utf-8')))

        return StepResults(1, get_results())


def zip_namelist(input_stream, result_queue: ResultQueue) -> list:
    """Synchronously Open ZIP, get information about files and return dumped json object."""
    if not zipfile.is_zipfile(input_stream):
        raise ValueError("Input stream is not a valid ZIP file.")

    detailed_info = {}
    with zipfile.ZipFile(input_stream, 'r') as zip_file:
       for info in zip_file.infolist():
           mime_type, _ = mimetypes.guess_type(info.filename)

           file_info = {
               'is_dir': info.is_dir(),
               'file_size': info.file_size,
               'modified_time': datetime(*info.date_time).strftime('%Y-%m-%d %H:%M:%S'),
               'compressed_size': info.compress_size,
               'compress_type': info.compress_type,
               'media_type': mime_type if mime_type and not info.is_dir() else ""
           }
           detailed_info[info.filename] = file_info

    return json.dumps(detailed_info, indent=4)
