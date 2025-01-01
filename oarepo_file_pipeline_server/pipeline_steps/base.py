import abc
import io

import aiohttp

# preview_zip, preview_picture, extract_file_zip, extract_directory_zip, crypt4gh add recipient, create_zip
# step create zip  if multiple inputs in last step

# args are from redis
# if first step then inputs are empty
# outputs from previous steps goes as inputs to next pipeline step

from oarepo_file_pipeline_server.pipeline_data.pipeline_data import PipelineData


class PipelineStep(abc.ABC):
    async def process(self, inputs: list[PipelineData], args: dict) -> list[PipelineData]:
        pass

    async def read_file_content_from_s3(self, source_url: str) -> io.BytesIO:
        async with aiohttp.ClientSession() as session:
            async with session.get(source_url) as response:
                if response.status != 200:
                    raise ValueError(f"Failed to fetch file from URL: {response.status}")
                content = await response.read()
                return io.BytesIO(content)