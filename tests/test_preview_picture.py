import io

import PIL
import aiohttp
import pytest
from PIL import Image

from oarepo_file_pipeline_server.pipeline_data.bytes_pipeline_data import BytesPipelineData
from oarepo_file_pipeline_server.pipeline_data.url_pipeline_data import UrlPipelineData
from oarepo_file_pipeline_server.pipeline_steps.preview_picture import PreviewPicture

@pytest.mark.asyncio(loop_scope="session")
async def test_preview_picture_success_from_inputs_bytes_to_smaller_size_jpg_from_bytes():
    with open("tests/files_for_tests/nando-jpeg-quality-001.jpg", "rb") as f:
        picture = io.BytesIO(f.read())
        print("Original picture stream size:", len(picture.getvalue()))

    async def get_data(picture_stream):
        yield BytesPipelineData({'media_type': 'image/jpeg'}, picture_stream)

    step = PreviewPicture()
    inputs = get_data(picture)

    picture_buffer = io.BytesIO()
    with Image.open(picture) as picture:
        picture.load()
        picture.thumbnail((800, 600))
        picture.save(picture_buffer, format=picture.format)


    outputs = await step.process(inputs, {
        'max_width': 800,
        'max_height': 600
    })
    output = await anext(outputs.results)

    assert output.metadata['media_type'] == 'image/jpeg'
    assert output.metadata['width'] == 800
    assert output.metadata['height'] == 600

    picture_bytes = await output.read()
    assert picture_bytes == picture_buffer.getvalue()

    with pytest.raises(StopAsyncIteration):
        await anext(outputs.results)

@pytest.mark.asyncio(loop_scope="session")
async def test_preview_picture_success_from_inputs_bytes_to_smaller_size_png_from_bytes():
    with open("tests/files_for_tests/CESNET.png", "rb") as f:
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
        picture.save(picture_buffer, format=picture.format)


    outputs = await step.process(inputs, {
        'max_width': 800,
        'max_height': 600
    })
    output = await anext(outputs.results)

    assert output.metadata['media_type'] == 'image/png'
    assert output.metadata['width'] == 800
    assert output.metadata['height'] == 533 # due to keep the aspect ratio the same

    picture_bytes = await output.read()
    assert picture_bytes == picture_buffer.getvalue()

    with pytest.raises(StopAsyncIteration):
        await anext(outputs.results)

@pytest.mark.asyncio(loop_scope="session")
async def test_preview_picture_success_from_inputs_bytes_same_picture_png_from_bytes():
    with open("tests/files_for_tests/CESNET.png", "rb") as f:
        picture = io.BytesIO(f.read())
        print("Original picture stream size:", len(picture.getvalue()))

    async def get_data(picture_stream):
        yield BytesPipelineData({'media_type': 'image/png'}, picture_stream)

    step = PreviewPicture()
    inputs = get_data(picture)
    outputs = await step.process(inputs, {
        'max_width': 3000,
        'max_height': 3000
    })
    output = await anext(outputs.results)

    assert output.metadata['media_type'] == 'image/png'
    assert output.metadata['width'] == 2400
    assert output.metadata['height'] == 1600

    picture_bytes = await output.read()
    assert picture_bytes == picture.getvalue()

    with pytest.raises(StopAsyncIteration):
        await anext(outputs.results)

@pytest.mark.asyncio(loop_scope="session")
async def test_preview_picture_success_from_inputs_bytes_same_picture_jpg_from_bytes():
    with open("tests/files_for_tests/nando-jpeg-quality-001.jpg", "rb") as f:
        picture = io.BytesIO(f.read())
        print("Original picture stream size:", len(picture.getvalue()))

    async def get_data(picture_stream):
        yield BytesPipelineData({'media_type': 'image/jpeg'}, picture_stream)

    step = PreviewPicture()
    inputs = get_data(picture)

    outputs = await step.process(inputs, {
        'max_width': 1280,
        'max_height': 960
    })
    output = await anext(outputs.results)

    assert output.metadata['media_type'] == 'image/jpeg'
    assert output.metadata['width'] == 1280
    assert output.metadata['height'] == 960

    picture_bytes = await output.read()
    assert picture_bytes == picture.getvalue() # should be exactly the same

    with pytest.raises(StopAsyncIteration):
        await anext(outputs.results)

@pytest.mark.asyncio(loop_scope="session")
async def test_preview_picture_fail_input_is_not_picture():
    with open("tests/files_for_tests/test_zip/test1.txt", "rb") as f:
        picture = io.BytesIO(f.read())

    async def get_data(picture_stream):
        yield BytesPipelineData({'media_type': 'image/jpeg'}, picture_stream)

    step = PreviewPicture()
    inputs = get_data(picture)

    outputs = await step.process(inputs, {
        'max_width': 1280,
        'max_height': 960
    })
    with pytest.raises(PIL.UnidentifiedImageError):
        await anext(outputs.results)

@pytest.mark.asyncio(loop_scope="session")
async def test_preview_picture_success_from_inputs_bytes_same_picture_jpg_url():
    with open("tests/files_for_tests/CESNET.png", "rb") as f:
        picture = io.BytesIO(f.read())
        print("Original picture stream size:", len(picture.getvalue()))

    async def get_data():
        session = aiohttp.ClientSession()
        yield UrlPipelineData("https://github.com/oarepo/oarepo-file-pipeline-server/blob/first-version/tests/files_for_tests/CESNET.png?raw=true", session)

    step = PreviewPicture()
    inputs = get_data()

    outputs = await step.process(inputs, {
        'max_width': 3000,
        'max_height': 3000
    })
    output = await anext(outputs.results)

    assert output.metadata['media_type'] == 'image/png'
    assert output.metadata['width'] == 2400
    assert output.metadata['height'] == 1600

    picture_bytes = await output.read()
    assert picture_bytes == picture.getvalue() # should be exactly the same

    with pytest.raises(StopAsyncIteration):
        await anext(outputs.results)

