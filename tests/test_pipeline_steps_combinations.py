import pytest

from oarepo_file_pipeline_server.pipeline_steps.extract_file_zip import ExtractFileZip
from oarepo_file_pipeline_server.pipeline_steps.preview_picture import PreviewPicture


@pytest.mark.asyncio(loop_scope="session")
async def test_pipeline_steps_extract_img_preview_picture():
    with open('tests/files_for_tests/test_zip_with_picture/App Center_001.png', 'rb') as f:
        picture = f.read()
    step1 = ExtractFileZip()
    step2 = PreviewPicture()

    outputs = step1.process(
        None,
        args={
            "source_url": "https://github.com/oarepo/oarepo-file-pipeline-server/raw/refs/heads/first-version/tests/files_for_tests/test_zip_with_picture.zip",
            'file_name': "test_zip_with_picture/App Center_001.png"
        }
    )

    final_outputs = step2.process(
        outputs,
        args={
            'max_width': 3000,
            'max_height': 3000,
        }
    )

    output = await anext(final_outputs)

    assert output.metadata['media_type'] == 'image/png'
    assert output.metadata['height'] == 852
    assert output.metadata['width'] == 1332

    picture_bytes = await output.read()
    assert picture_bytes == picture

    with pytest.raises(StopAsyncIteration):
        await anext(outputs)

    with pytest.raises(StopAsyncIteration):
        await anext(final_outputs)