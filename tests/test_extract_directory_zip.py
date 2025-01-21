import io
import aiohttp
import pytest

from oarepo_file_pipeline_server.pipeline_data.bytes_pipeline_data import BytesPipelineData
from oarepo_file_pipeline_server.pipeline_data.url_pipeline_data import UrlPipelineData
from oarepo_file_pipeline_server.pipeline_steps.extract_directory_zip import ExtractDirectoryZip


@pytest.mark.asyncio(loop_scope="session")
async def test_extract_directory_zip_success_from_url():
    step = ExtractDirectoryZip()
    outputs = step.process(None,
            args={
                "source_url": "https://github.com/oarepo/oarepo-file-pipeline-server/raw/refs/heads/first-version/tests/files_for_tests/test_directory_zip.zip",
                "directory_name": "test_directory_zip/directory1/"
            })

    output = await anext(outputs)
    buffer = io.BytesIO()
    async for content in output:
        if isinstance(content, dict):
            assert {
                       'file_name': 'directory1-file1.txt',
                       'media_type': 'text/plain'
                   } == content
        elif content == b'':
            break
        else:
            buffer.write(content)
    assert buffer.getvalue() == b'directory1-file1\n'

    output = await anext(outputs)
    buffer = io.BytesIO()
    async for content in output:
        if isinstance(content, dict):
            assert {
                       'file_name': 'directory1-file2.txt',
                       'media_type': 'text/plain'
                   } == content
        elif content == b'':
            break
        else:
            buffer.write(content)
    assert buffer.getvalue() == b'directory1-file2\n'

    with pytest.raises(StopAsyncIteration):
        await anext(outputs)

@pytest.mark.asyncio(loop_scope="session")
async def test_extract_file_zip_success_from_input_bytes():
    async def get_data():
        with open("tests/files_for_tests/test_directory_zip.zip", "rb") as f:
            zip_stream = io.BytesIO(f.read())
        yield BytesPipelineData({'media_type': 'application/zip'}, zip_stream)

    inputs = get_data()
    step = ExtractDirectoryZip()

    outputs = step.process(inputs, args={'directory_name': 'test_directory_zip/directory2/'})
    output = await anext(outputs)
    buffer = io.BytesIO()
    async for content in output:
        buffer.write(content)
    assert buffer.getvalue() == b'directory2-file2\n'

    output = await anext(outputs)
    buffer = io.BytesIO()
    async for content in output:
        buffer.write(content)
    assert buffer.getvalue() == b'directory2-file1\n'

    with pytest.raises(StopAsyncIteration):
        await anext(outputs)

@pytest.mark.asyncio(loop_scope="session")
async def test_extract_file_zip_success_from_input_url():
    async def get_data():
        session = aiohttp.ClientSession()
        yield UrlPipelineData('https://github.com/oarepo/oarepo-file-pipeline-server/raw/refs/heads/first-version/tests/files_for_tests/test_directory_zip.zip', session)

    inputs = get_data()
    step = ExtractDirectoryZip()

    outputs = step.process(inputs, args={'directory_name': 'test_directory_zip/directory2/'})
    output = await anext(outputs)

    assert output.metadata['file_name'] == 'directory2-file2.txt'
    buffer = io.BytesIO()
    async for content in output:
        buffer.write(content)
    assert buffer.getvalue() == b'directory2-file2\n'

    output = await anext(outputs)
    assert output.metadata['file_name'] == 'directory2-file1.txt'
    buffer = io.BytesIO()
    async for content in output:
        buffer.write(content)
    assert buffer.getvalue() == b'directory2-file1\n'

    with pytest.raises(StopAsyncIteration):
        await anext(outputs)


@pytest.mark.asyncio(loop_scope="session")
async def test_extract_file_zip_success_from_input_bytes_1000_files():
    async def get_data():
        with open("tests/files_for_tests/1000_files.zip", "rb") as f:
            zip_stream = io.BytesIO(f.read())
        yield BytesPipelineData({'media_type': 'application/zip'}, zip_stream)

    inputs = get_data()
    step = ExtractDirectoryZip()

    outputs = step.process(inputs, args={'directory_name': '1000_files/1000_files_zip'})

    for file_index in range(1, 1001):
        output = await anext(outputs)
        buffer = io.BytesIO()
        print(f'Getting: {file_index}')
        async for content in output:
            buffer.write(content)


        file_content = buffer.getvalue()
        assert len(file_content) == 10 * 1024 * 1024, f"File {file_index} does not have a size of 10MB"
        assert file_content == b'0\n' * (10 * 1024 * 1024 // 2), f"File {file_index} content is not all zeros"

    with pytest.raises(StopAsyncIteration):
        await anext(outputs)


@pytest.mark.asyncio(loop_scope="session")
async def test_extract_directory_zip_success_from_url_1000_files():
    with open('tests/files_for_tests/10mb_zero_file.txt', 'rb') as f:
        expected_file_content = f.read()

    step = ExtractDirectoryZip()
    outputs = step.process(None,
                           args={
                               "source_url": "https://github.com/oarepo/oarepo-file-pipeline-server/raw/refs/heads/first-version/tests/files_for_tests/1000_files.zip",
                               "directory_name": "1000_files/1000_files_zip"
                           })

    for i in range(1,1001):
        output = await anext(outputs)
        buffer = io.BytesIO()

        assert output.metadata['file_name'] == f'file_{i}.txt'
        assert output.metadata['media_type'] == 'text/plain'

        async for content in output:
            buffer.write(content)
        assert buffer.getvalue() == expected_file_content, f'Failed at file_{i}'

    with pytest.raises(StopAsyncIteration):
        await anext(outputs)
