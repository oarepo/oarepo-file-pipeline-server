import io
import unittest

import aiohttp
import pytest

from oarepo_file_pipeline_server.pipeline_data.bytes_pipeline_data import BytesPipelineData
from oarepo_file_pipeline_server.pipeline_data.url_pipeline_data import UrlPipelineData
from oarepo_file_pipeline_server.pipeline_steps.extract_zip import ExtractZip


@pytest.mark.asyncio(loop_scope="session")
async def test_extract_file_zip_success_from_url():
    step = ExtractZip()

    outputs = await step.process(None,
            args={
                "source_url": "https://github.com/oarepo/oarepo-file-pipeline-server/raw/refs/heads/first-version/tests/files_for_tests/test_zip.zip",
                "directory_or_file_name": "test_zip/test1.txt"
            })

    output = await anext(outputs.results)

    assert output.metadata['file_name'] == 'test1.txt'
    assert output.metadata['media_type'] == 'text/plain'
    buffer = io.BytesIO()
    async for content in output:
        buffer.write(content)
    assert buffer.getvalue() == b'Hello World\n'

    with pytest.raises(StopAsyncIteration):
        await anext(outputs.results)

@pytest.mark.asyncio(loop_scope="session")
async def test_extract_file_zip_success_from_input_bytes():
    with open("tests/files_for_tests/test_zip.zip", "rb") as f:
        zip_stream = io.BytesIO(f.read())

    async def get_data():
        yield BytesPipelineData({'media_type': 'application/zip'}, zip_stream)

    inputs = get_data()

    step = ExtractZip()
    outputs = await step.process(inputs, args={'directory_or_file_name': "test_zip/test1.txt"})
    output = await anext(outputs.results)

    assert output.metadata['file_name'] == 'test1.txt'
    assert output.metadata['media_type'] == 'text/plain'
    buffer = io.BytesIO()
    async for content in output:
        buffer.write(content)
    assert buffer.getvalue() == b'Hello World\n'

    with pytest.raises(StopAsyncIteration):
        await anext(outputs.results)


@pytest.mark.asyncio(loop_scope="session")
async def test_extract_file_zip_success_from_input_url():
    session = aiohttp.ClientSession()
    async def get_data():
        yield UrlPipelineData("https://github.com/oarepo/oarepo-file-pipeline-server/raw/refs/heads/first-version/tests/files_for_tests/test_zip.zip",session)

    inputs = get_data()
    step = ExtractZip()
    outputs = await step.process(inputs,args={'directory_or_file_name': "test_zip/test1.txt"})
    output = await anext(outputs.results)

    assert output.metadata['file_name'] == 'test1.txt'
    assert output.metadata['media_type'] == 'text/plain'
    buffer = io.BytesIO()
    async for content in output:
        buffer.write(content)
    assert buffer.getvalue() == b'Hello World\n'

    with pytest.raises(StopAsyncIteration):
        await anext(outputs.results)

@pytest.mark.asyncio(loop_scope="session")
async def test_extract_file_fail_no_inputs_no_args():
    step = ExtractZip()

    with pytest.raises(ValueError, match='No input or arguments were provided to ExtractZip step'):
        outputs = await step.process(None,{})
        await anext(outputs.results)

@pytest.mark.asyncio(loop_scope="session")
async def test_extract_file_fail_no_file_name():
    step = ExtractZip()
    with pytest.raises(ValueError):
        outputs = await step.process(None,{'source_url' : 'something'})
        await anext(outputs.results)



@pytest.mark.asyncio(loop_scope="session")
async def test_extract_file_fail_non_existing_file_name():
    step = ExtractZip()

    with open("tests/files_for_tests/test_zip.zip", "rb") as f:
        zip_stream = io.BytesIO(f.read())
    async def get_data():
        yield BytesPipelineData({'media_type': 'application/zip'}, zip_stream)
    with pytest.raises(ValueError):
        outputs = await step.process(get_data(), args={'directory_or_file_name': "test_zip/test1.tx"})
        await anext(outputs.results)

@pytest.mark.asyncio(loop_scope="session")
async def test_preview_zip_invalid_inputs():
    step = ExtractZip()
    with open("tests/files_for_tests/test_zip/test1.txt", "rb") as f:
        text_stream = io.BytesIO(f.read())
    async def get_data():
        yield BytesPipelineData({'media_type': 'application/zip'}, text_stream)

    with pytest.raises(ValueError,match="Input stream is not a valid ZIP file."):
        outputs = await step.process(get_data(), args={'directory_or_file_name': "test_zip/test1.txt"})
        await anext(outputs.results)


"""
class Test(unittest.IsolatedAsyncioTestCase):
    async def test_extract_file_fail_no_file_name(self):
        step = ExtractZip()

        outputs = await step.process(None,{'source_url' : 'something'})
        with pytest.raises(ValueError):
            await anext(outputs.results)

    async def test_extract_file_fail_non_existing_file_name(self):
        step = ExtractZip()

        with open("files_for_tests/test_zip.zip", "rb") as f:
            zip_stream = io.BytesIO(f.read())

        async def get_data():
            yield BytesPipelineData({'media_type': 'application/zip'}, zip_stream)

        outputs = await step.process(get_data(), args={'directory_or_file_name': "test_zip/test1.tx"})
        with pytest.raises(ValueError, match="File 'test_zip/test1.tx' not found in the ZIP archive."):
            await anext(outputs.results)
"""