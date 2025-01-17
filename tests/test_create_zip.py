import io
import unittest
import zipfile

import aiohttp
import pytest

from oarepo_file_pipeline_server.pipeline_data.bytes_pipeline_data import BytesPipelineData
from oarepo_file_pipeline_server.pipeline_data.queue_pipeline_data import QueuePipelineData
from oarepo_file_pipeline_server.pipeline_data.url_pipeline_data import UrlPipelineData
from oarepo_file_pipeline_server.pipeline_steps.create_zip import CreateZip



@pytest.mark.asyncio(loop_scope="session")
async def test_create_zip_success_from_url():
    async def get_data():
        session = aiohttp.ClientSession()
        yield UrlPipelineData('https://github.com/oarepo/oarepo-file-pipeline-server/raw/refs/heads/first-version/tests/files_for_tests/test_directory_zip/directory1/directory1-file1.txt', session)
        yield UrlPipelineData('https://github.com/oarepo/oarepo-file-pipeline-server/raw/refs/heads/first-version/tests/files_for_tests/test_directory_zip/directory1/directory1-file2.txt', session)


    inputs = get_data()
    step = CreateZip()

    outputs = step.process(inputs, args={})
    output = await anext(outputs)
    chunks = b''
    chunks += await output.read()

    with pytest.raises(StopAsyncIteration):
        await anext(outputs)

    with zipfile.ZipFile(io.BytesIO(chunks)) as zf:
        for file_name in zf.namelist():
            with zf.open(file_name) as file:
                content = file.read()

                if file_name == 'file_0':
                    assert content == b'directory1-file1\n'
                elif file_name == 'file_1':
                    assert content == b'directory1-file2\n'
                else:
                    raise ValueError(f'Unexpected file in zip: {file_name}')


@pytest.mark.asyncio(loop_scope="session")
async def test_extract_file_zip_success_from_input_bytes():
    with open('tests/files_for_tests/test_directory_zip/directory1/directory1-file1.txt', 'rb') as file:
        file1 = file.read()

    with open('tests/files_for_tests/test_directory_zip/directory1/directory1-file2.txt', 'rb') as file:
        file2 = file.read()

    async def get_data(file1_bytes, file2_bytes):
        yield BytesPipelineData({
            'media_type': 'text/plain',
            'file_name': 'test_directory_zip/directory1/directory1-file1.txt',
        }, io.BytesIO(file1_bytes))

        yield BytesPipelineData({
            'media_type': 'text/plain',
            'file_name': 'test_directory_zip/directory1/directory1-file2.txt',
        }, io.BytesIO(file2_bytes))

    inputs = get_data(file1, file2)
    step = CreateZip()

    outputs = step.process(inputs, args={})
    output = await anext(outputs)
    chunks = b''
    chunks += await output.read()

    with pytest.raises(StopAsyncIteration):
        await anext(outputs)

    with zipfile.ZipFile(io.BytesIO(chunks)) as zf:
        for file_name in zf.namelist():
            with zf.open(file_name) as file:
                content = file.read()

                if file_name == 'test_directory_zip/directory1/directory1-file1.txt':
                    assert content == file1
                elif file_name == 'test_directory_zip/directory1/directory1-file2.txt':
                    assert content == file2
                else:
                    raise ValueError(f'Unexpected file in zip: {file_name}')


@pytest.mark.asyncio(loop_scope="session")
async def test_create_zip_no_inputs():
    step = CreateZip()

    with pytest.raises(ValueError, match="No input data provided to CreateZip step."):
        output = step.process(None, args={})
        await anext(output)

