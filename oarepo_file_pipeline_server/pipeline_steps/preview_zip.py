import io
import zipfile
from typing import AsyncIterator

import aiohttp

from oarepo_file_pipeline_server.async_to_sync.sync_runner import sync_stream_runner, ResultQueue, read_result
from oarepo_file_pipeline_server.pipeline_data.bytes_pipeline_data import BytesPipelineData
from oarepo_file_pipeline_server.pipeline_data.url_pipeline_data import UrlPipelineData
from oarepo_file_pipeline_server.pipeline_steps.base import PipelineStep
from oarepo_file_pipeline_server.pipeline_data.pipeline_data import PipelineData

class PreviewZip(PipelineStep):
    async def process(self, inputs: AsyncIterator[PipelineData] | None, args: dict) -> AsyncIterator[PipelineData] | None:
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
        namelist = await read_result(results)
        output = BytesPipelineData({'media_type': 'text/plain'}, io.BytesIO("\n".join(namelist).encode("utf-8")))
        yield output


def zip_namelist(input_stream, result_queue: ResultQueue):
    if not zipfile.is_zipfile(input_stream):
        raise ValueError("Input stream is not a valid ZIP file.")

    with zipfile.ZipFile(input_stream, 'r') as zip_file:
       return zip_file.namelist()
