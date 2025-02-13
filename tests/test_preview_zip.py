import io
import json

import aiohttp
import pytest

from oarepo_file_pipeline_server.pipeline_data.bytes_pipeline_data import BytesPipelineData
from oarepo_file_pipeline_server.pipeline_data.url_pipeline_data import UrlPipelineData
from oarepo_file_pipeline_server.pipeline_steps.preview_zip import PreviewZip

@pytest.mark.asyncio(loop_scope="session")
async def test_preview_zip_success_from_url():
    step = PreviewZip()

    async with aiohttp.ClientSession() as session:
        outputs = await step.process(
            None,
            args={
                "source_url": "https://github.com/oarepo/oarepo-file-pipeline-server/raw/refs/heads/first-version/tests/files_for_tests/test_zip.zip",
                "session": session,
            }
        )

        output = await anext(outputs.results)
        assert output.metadata == {'media_type': 'application/json'}
        buffer = await output.read()
        result = json.loads(buffer.decode('utf8'))
        assert result == {
            "test_zip/": {
                "is_dir": True,
                "file_size": 0,
                "modified_time": "2024-12-23 16:21:22",
                "compressed_size": 0,
                "compress_type": 0,
                "media_type": ""
            },
            "test_zip/test1.txt": {
                "is_dir": False,
                "file_size": 12,
                "modified_time": "2024-12-23 16:21:38",
                "compressed_size": 14,
                "compress_type": 8,
                "media_type": "text/plain"
            }
        }

        assert await output.read() == b''

        with pytest.raises(StopAsyncIteration):
            await anext(outputs.results)




@pytest.mark.asyncio(loop_scope="session")
async def test_preview_zip_success_from_inputs_bytes():
    with open("tests/files_for_tests/test_zip.zip", "rb") as f:
        zip_stream = io.BytesIO(f.read())

    async def get_data():
        yield BytesPipelineData({'media_type': 'application/zip'}, zip_stream)

    inputs = get_data()
    step = PreviewZip()

    outputs = await step.process(inputs, args={})
    output = await anext(outputs.results)

    assert output.metadata == {'media_type': 'application/json'}

    buffer = await output.read()
    result = json.loads(buffer.decode('utf8'))
    assert result == {
        "test_zip/": {
            "is_dir": True,
            "file_size": 0,
            "modified_time": "2024-12-23 16:21:22",
            "compressed_size": 0,
            "compress_type": 0,
            "media_type": ""
        },
        "test_zip/test1.txt": {
            "is_dir": False,
            "file_size": 12,
            "modified_time": "2024-12-23 16:21:38",
            "compressed_size": 14,
            "compress_type": 8,
            "media_type": "text/plain"
        }
    }

    assert await output.read() == b''


    with pytest.raises(StopAsyncIteration):
        await anext(outputs.results)

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

        outputs = await step.process(inputs, args={})
        output = await anext(outputs.results)

        assert output.metadata == {'media_type': 'application/json'}
        buffer = await output.read()
        result = json.loads(buffer.decode('utf8'))
        assert result == {
            "test_zip/": {
                "is_dir": True,
                "file_size": 0,
                "modified_time": "2024-12-23 16:21:22",
                "compressed_size": 0,
                "compress_type": 0,
                "media_type": ""
            },
            "test_zip/test1.txt": {
                "is_dir": False,
                "file_size": 12,
                "modified_time": "2024-12-23 16:21:38",
                "compressed_size": 14,
                "compress_type": 8,
                "media_type": "text/plain"
            }
        }

        assert await output.read() == b''

        with pytest.raises(StopAsyncIteration):
            await anext(outputs.results)

@pytest.mark.asyncio(loop_scope="session")
async def test_preview_zip_invalid_inputs():
    step = PreviewZip()

    with open("tests/files_for_tests/test_zip/test1.txt", "rb") as f:
        text_stream = io.BytesIO(f.read())

    async def get_data():
        yield BytesPipelineData({'media_type': 'application/zip'}, text_stream)

    with pytest.raises(ValueError, match="Input stream is not a valid ZIP file."):
        outputs = await step.process(get_data(), args={})
        await anext(outputs.results)

@pytest.mark.asyncio(loop_scope="session")
async def test_preview_zip_fail_no_inputs_no_args():
    step = PreviewZip()

    with pytest.raises(ValueError, match="No input or arguments were provided to PreviewZip step."):
        async for _ in (await step.process(None, args={})).results:
            pass

@pytest.mark.asyncio(loop_scope="session")
async def test_preview_zip_invalid_url():
    step = PreviewZip()
    with pytest.raises(ValueError):
        outputs = await step.process(
            None,
            args={
                "source_url": "https://github.com/oarepo/oarepo-file-pipeline-server/raw/refs/heads/first-version/tests/files_for_tests/test_zip.zp",
            }
        )
        await anext(outputs.results)