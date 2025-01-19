import io
from unittest.mock import AsyncMock, patch

import aiohttp
import pytest

from oarepo_file_pipeline_server.pipeline_data.bytes_pipeline_data import BytesPipelineData
from oarepo_file_pipeline_server.pipeline_data.url_pipeline_data import UrlPipelineData
from oarepo_file_pipeline_server.pipeline_steps.preview_zip import PreviewZip

@pytest.mark.asyncio(loop_scope="session")
async def test_preview_zip_success_from_url():
    step = PreviewZip()

    async with aiohttp.ClientSession() as session:
        outputs = step.process(
            None,
            args={
                "source_url": "https://github.com/oarepo/oarepo-file-pipeline-server/raw/refs/heads/first-version/tests/files_for_tests/test_zip.zip",
                "session": session,
            }
        )

        output = await anext(outputs)
        assert output.metadata == {'media_type': 'text/plain'}

        assert await output.read() == b'test_zip/\ntest_zip/test1.txt'
        assert await output.read() == b''

        with pytest.raises(StopAsyncIteration):
            await anext(outputs)




@pytest.mark.asyncio(loop_scope="session")
async def test_preview_zip_success_from_inputs_bytes():
    with open("tests/files_for_tests/test_zip.zip", "rb") as f:
        zip_stream = io.BytesIO(f.read())

    async def get_data():
        yield BytesPipelineData({'media_type': 'application/zip'}, zip_stream)

    inputs = get_data()
    step = PreviewZip()

    outputs = step.process(inputs, args={})
    output = await anext(outputs)

    assert output.metadata == {'media_type': 'text/plain'}
    assert await output.read() == b'test_zip/\ntest_zip/test1.txt'
    assert await output.read() == b''

    with pytest.raises(StopAsyncIteration):
        await anext(outputs)

@pytest.mark.asyncio(loop_scope="session")
async def test_preview_zip_success_from_inputs_url():
    async with aiohttp.ClientSession() as session:
        async def get_data():
            yield UrlPipelineData(
                "https://github.com/oarepo/oarepo-file-pipeline-server/raw/refs/heads/first-version/tests/files_for_tests/test_zip.zip",
                session
            )

        step = PreviewZip()
        inputs = get_data()

        outputs = step.process(inputs, args={})
        output = await anext(outputs)

        assert output.metadata == {'media_type': 'text/plain'}
        assert await output.read() == b'test_zip/\ntest_zip/test1.txt'
        assert await output.read() == b''

        with pytest.raises(StopAsyncIteration):
            await anext(outputs)

@pytest.mark.asyncio(loop_scope="session")
async def test_preview_zip_invalid_inputs():
    step = PreviewZip()

    with open("tests/files_for_tests/test_zip/test1.txt", "rb") as f:
        text_stream = io.BytesIO(f.read())

    async def get_data():
        yield BytesPipelineData({'media_type': 'application/zip'}, text_stream)

    with pytest.raises(ValueError, match="Input stream is not a valid ZIP file."):
        outputs = step.process(get_data(), args={})
        await anext(outputs)

@pytest.mark.asyncio(loop_scope="session")
async def test_preview_zip_fail_no_inputs_no_args():
    step = PreviewZip()

    with pytest.raises(ValueError, match="No input or arguments were provided to PreviewZip step."):
        async for _ in step.process(None, args={}):
            pass

@pytest.mark.asyncio(loop_scope="session")
async def test_preview_zip_invalid_url():
    step = PreviewZip()
    with pytest.raises(ValueError):
        outputs = step.process(
            None,
            args={
                "source_url": "https://github.com/oarepo/oarepo-file-pipeline-server/raw/refs/heads/first-version/tests/files_for_tests/test_zip.zp",
            }
        )
        await anext(outputs)