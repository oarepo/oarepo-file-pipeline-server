import abc
from typing import AsyncIterator, Self


# preview_zip, preview_picture, extract_file_zip, extract_directory_zip, crypt4gh add recipient, create_zip
# step create zip  if multiple inputs in last step

# args are from redis
# if first step then inputs are empty
# outputs from previous steps goes as inputs to next pipeline step

from oarepo_file_pipeline_server.pipeline_data.pipeline_data import PipelineData

class PipelineStep(abc.ABC):
    async def process(self, inputs: AsyncIterator[PipelineData] | None, args: dict) -> AsyncIterator[PipelineData] | None:
        pass

