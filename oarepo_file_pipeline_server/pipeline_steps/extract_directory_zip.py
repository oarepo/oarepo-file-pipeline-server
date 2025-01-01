import io
import zipfile

from oarepo_file_pipeline_server.pipeline_steps.base import PipelineStep
from oarepo_file_pipeline_server.pipeline_data.pipeline_data import PipelineData

class ExtractDirectoryZip(PipelineStep):
    async def process(self, inputs: list[PipelineData], args: dict) -> list[PipelineData]:
        """
        Extracts a directory zip from the input zip file.
        """
        if not inputs and not args:
            raise ValueError("No input or arguments were provided to PreviewZip step.")
        if inputs:
            input_stream = inputs.pop(0).get_stream()  # maybe change to deque for better time complexity
            print("Received file from inputs list")
        elif args and "source_url" in args:
            input_stream = await self.read_file_content_from_s3(args["source_url"])
            print("Read file from s3")
        else:
            raise ValueError("No input nor source_url were provided.")

        if not zipfile.is_zipfile(input_stream):
            raise ValueError("Input stream is not a valid ZIP file.")

        directory_name = args.get("directory_name")
        if not directory_name:
            raise ValueError("Directory name to extract is not specified in arguments.")

        extracted_files = []
        with zipfile.ZipFile(input_stream, 'r') as zip_file:
            # Normalize directory name to ensure it ends with 1 '/'
            # in case user made a typo with 2 backslashes instead of 1 etc.
            directory_name = directory_name.rstrip("/") + "/"

            for file_name in zip_file.namelist():
                if file_name.startswith(directory_name) and not file_name.endswith("/"):  # Exclude subdirectories
                    with zip_file.open(file_name, 'r') as extracted_file:
                        file_content = extracted_file.read()
                        print(f'Extracting {file_name}...')
                        current_file = PipelineData(io.BytesIO(file_content))
                        # TODO maybe add file_type to metadata
                        # Could deduce file type by looking at file extension but in some cases there will be no extension
                        current_file.metadata = {'file_name': file_name}
                        extracted_files.append(current_file)

        if not extracted_files:
            raise ValueError(f"No files found in directory '{directory_name}' in the ZIP archive.")

        return extracted_files


