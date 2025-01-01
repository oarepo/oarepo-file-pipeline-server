import io
import zipfile

from oarepo_file_pipeline_server.pipeline_steps.base import PipelineStep
from oarepo_file_pipeline_server.pipeline_data.pipeline_data import PipelineData

class ExtractFileZip(PipelineStep):
    async def process(self, inputs: list[PipelineData], args: dict) -> list[PipelineData]:
        """
        Extract specific file from zip archive.
        """

        if not inputs and not args:
            raise ValueError("No input or arguments were provided to PreviewZip step.")
        if inputs:
            input_stream = inputs.pop(0).get_stream()
            print(f"Received file from inputs list")
        elif args and "source_url" in args:
            input_stream = await self.read_file_content_from_s3(args["source_url"])
            print(f"Read file from s3")
        else:
            raise ValueError("No input provided.")

        if input_stream is None:
            raise ValueError("Input stream cannot be None.")

        if not zipfile.is_zipfile(input_stream):
            raise ValueError("Input stream is not a valid ZIP file.")

        file_name = args.get("file_name")
        if not file_name:
            raise ValueError("No file name to extract was provided.")

        with zipfile.ZipFile(input_stream, 'r') as zip_file:
            if file_name not in zip_file.namelist():
                raise ValueError(f"File '{file_name}' not found in the ZIP archive.")

            with zip_file.open(file_name, 'r') as extracted_file:
                file_content = extracted_file.read()

        output = PipelineData(io.BytesIO(file_content))
        output.metadata = {'media_type': 'image/png'} # TODO change to real format, dont know how to determine what file type is it
        return [output]

