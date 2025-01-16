import io
import unittest
import pytest
from PIL import Image

from oarepo_file_pipeline_server.pipeline_data.bytes_pipeline_data import BytesPipelineData
from oarepo_file_pipeline_server.pipeline_steps.preview_picture import PreviewPicture


class TestPreviewPicture(unittest.IsolatedAsyncioTestCase):
    async def test_preview_picture_success_from_inputs_bytes_to_smaller_size(self):
        with open("files_for_tests/nando-jpeg-quality-001.jpg", "rb") as f:
            picture = io.BytesIO(f.read())
            print("Original picture stream size:", len(picture.getvalue()))

        async def get_data(picture_stream):
            yield BytesPipelineData({'media_type': 'image/png'}, picture_stream)

        step = PreviewPicture()
        inputs = get_data(picture)

        picture_buffer = io.BytesIO()
        with Image.open(picture) as picture:
            picture.load()
            picture.thumbnail((800, 600))
            picture.save(picture_buffer, format=picture.format, quality='keep')


        outputs = step.process(inputs, {
            'max_width': 800,
            'max_height': 600
        })
        output = await anext(outputs)

        assert output.metadata['media_type'] == 'image/jpeg'
        assert output.metadata['width'] == 800
        assert output.metadata['height'] == 600

        picture_bytes = await output.read()
        assert picture_bytes == picture_buffer.getvalue()

        with pytest.raises(StopAsyncIteration):
            await anext(outputs)


    async def test_preview_picture_success_from_inputs_bytes_same_picture(self):
        with open("files_for_tests/nando-jpeg-quality-001.jpg", "rb") as f:
            picture = io.BytesIO(f.read())
            print("Original picture stream size:", len(picture.getvalue()))

        async def get_data(picture_stream):
            yield BytesPipelineData({'media_type': 'image/png'}, picture_stream)

        step = PreviewPicture()
        inputs = get_data(picture)

        picture_buffer = io.BytesIO()
        with Image.open(picture) as picture:
            picture.load()
            picture.thumbnail((1280, 960))
            picture.save(picture_buffer, format=picture.format, quality='keep')


        outputs = step.process(inputs, {
            'max_width': 1280,
            'max_height': 960
        })
        output = await anext(outputs)

        assert output.metadata['media_type'] == 'image/jpeg'
        assert output.metadata['width'] == 1280
        assert output.metadata['height'] == 960

        picture_bytes = await output.read()
        assert picture_bytes == picture_buffer.getvalue()

        with pytest.raises(StopAsyncIteration):
            await anext(outputs)


if __name__ == "__main__":
    unittest.main()

