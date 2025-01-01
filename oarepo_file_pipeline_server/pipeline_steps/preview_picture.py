from PIL import Image
from oarepo_file_pipeline_server.pipeline_steps.base import PipelineStep
from oarepo_file_pipeline_server.pipeline_data.pipeline_data import PipelineData

class PreviewPicture(PipelineStep):
    async def process(self, inputs: list[PipelineData], args: dict) -> list[PipelineData]:
        if not inputs and not args:
            raise ValueError("No input or arguments were provided to PreviewPicture step.")
        if inputs:
            input_stream = inputs.pop(0).get_stream()  # maybe change to deque for better time complexity
            print("Received picture from inputs list")
        elif args and "source_url" in args:
            input_stream = await self.read_file_content_from_s3(args["source_url"])
            print("Read picture from s3")
        else:
            raise ValueError("No input provided.")

        try:
            image = Image.open(input_stream)
            image.verify()  # Verify the image is valid

            # TODO image manipulation ( crop, resize etc...)

            outputs = PipelineData(input_stream)
            outputs.metadata = {'media_type': f'image/{image.format.lower()}'}
            return [outputs]
        except Exception as e:
            raise ValueError(f"Input stream is not a valid image: {str(e)}")
