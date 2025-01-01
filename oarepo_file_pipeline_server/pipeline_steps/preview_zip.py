import io
import zipfile

from oarepo_file_pipeline_server.pipeline_steps.base import PipelineStep
from oarepo_file_pipeline_server.pipeline_data.pipeline_data import PipelineData

class PreviewZip(PipelineStep):
    async def process(self, inputs: list[PipelineData], args: dict) -> list[PipelineData]:
        if not inputs and not args:
            raise ValueError("No input or arguments were provided to PreviewZip step.")
        if inputs:
            input_stream = inputs.pop(0).get_stream()  # maybe change to deque for better time complexity
        elif args and "source_url" in args:
            input_stream = await self.read_file_content_from_s3(args["source_url"])
        else:
            raise ValueError("No input or source_link were provided.")

        if input_stream is None:
            raise ValueError("Input stream cannot be None.")

        if not zipfile.is_zipfile(input_stream):
            raise ValueError("Input stream is not a valid ZIP file.")

        with zipfile.ZipFile(input_stream, 'r') as zip_file:
            file_list = zip_file.namelist()
            summary = "\n".join(file_list).encode('utf-8')

        outputs = PipelineData(io.BytesIO(summary))
        outputs.metadata = {'media_type': 'text/plain'}
        return [outputs]



