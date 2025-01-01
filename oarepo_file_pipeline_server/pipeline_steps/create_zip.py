import io
import zipfile

from oarepo_file_pipeline_server.pipeline_steps.base import PipelineStep
from oarepo_file_pipeline_server.pipeline_data.pipeline_data import PipelineData


class CreateZip(PipelineStep):
    async def process(self, inputs: list[PipelineData], args: dict) -> list[PipelineData]:
        """
        Create zip file from inputs list.
        """
        if not inputs:
            raise ValueError("No input data provided to CreateZip step.")

        file_names = [
            inputs[i].metadata.get('file_name', f"file_{i}")
            for i in range(len(inputs))
        ]

        zip_stream = io.BytesIO()
        with zipfile.ZipFile(zip_stream, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            for pipeline_data, file_name in zip(inputs, file_names):
                # Write each input stream to the ZIP file
                input_stream = pipeline_data.get_stream()
                zip_file.writestr(file_name, input_stream.read())

        zip_stream.seek(0)  # Reset stream position for reading
        outputs = PipelineData(zip_stream)
        outputs.metadata = {
            'media_type': 'application/zip',
            'headers': {'Content-Disposition': f'attachment; filename="my_zip.zip"'}
        }
        return [outputs]

