import io
import zipfile
from unittest.mock import patch, AsyncMock, MagicMock

import pytest

from oarepo_file_pipeline_server.pipeline_steps.preview_zip import PreviewZip
from oarepo_file_pipeline_server.pipeline_data.pipeline_data import PipelineData

@pytest.mark.asyncio
async def test_preview_zip_success():
    zip_stream = io.BytesIO()
    with zipfile.ZipFile(zip_stream, 'w') as zf:
        zf.writestr('test1.txt', 'Hello World')
        zf.writestr('test2.txt', 'FastAPI Pipeline')

    zip_stream.seek(0)  # Reset stream to the beginning
    input_data = PipelineData(zip_stream)

    step = PreviewZip()
    outputs = await step.process([input_data], args={})

    output_stream = outputs[0].get_stream()
    assert output_stream.read().decode('utf-8') == 'test1.txt\ntest2.txt'

@pytest.mark.asyncio
async def test_preview_zip_mock_s3():
    zip_file_path = "test_zip.zip"
    with open(zip_file_path, "rb") as f:
        zip_stream = io.BytesIO(f.read())

    with patch.object(PreviewZip, 'read_file_content_from_s3', AsyncMock(return_value=zip_stream)):
        step = PreviewZip()

        args = {"source_url": "some_url_s3"}

        outputs = await step.process([], args=args)

        output_stream = outputs[0].get_stream()
        file_list = output_stream.read().decode('utf-8')

        with zipfile.ZipFile(zip_file_path, 'r') as zf:
            expected_file_list = "\n".join(zf.namelist())

        assert file_list == expected_file_list
        assert outputs[0].metadata['media_type'] == 'text/plain'

@pytest.mark.asyncio
async def test_preview_zip_error_response_mock_s3():
    with patch.object(PreviewZip, 'read_file_content_from_s3', AsyncMock(return_value=None)):
        step = PreviewZip()
        args = {"source_url": "some_url_s3"}

        with pytest.raises(ValueError, match="Input stream cannot be None."):
            await step.process([], args=args)

@pytest.mark.asyncio
async def test_preview_zip_no_input_no_args():
    step = PreviewZip()
    with pytest.raises(ValueError, match="No input or arguments were provided to PreviewZip step."):
        await step.process([], args={})

@pytest.mark.asyncio
async def test_preview_zip_not_valid_zip():
    some_file = io.BytesIO()
    with open("test_zip/test1.txt", "rb") as f:
        text_stream = io.BytesIO(f.read())

    text_stream.seek(0)
    input_data = PipelineData(text_stream)

    step = PreviewZip()

    with pytest.raises(ValueError, match="Input stream is not a valid ZIP file."):
         await step.process([input_data], args={})
