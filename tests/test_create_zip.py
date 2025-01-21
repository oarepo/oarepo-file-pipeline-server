import asyncio
import io
import zipfile
from stat import S_IFREG
import aiohttp
import pytest

from oarepo_file_pipeline_server.pipeline_data.bytes_pipeline_data import BytesPipelineData
from oarepo_file_pipeline_server.pipeline_data.queue_pipeline_data import QueuePipelineData
from oarepo_file_pipeline_server.pipeline_data.url_pipeline_data import UrlPipelineData
from oarepo_file_pipeline_server.pipeline_steps.create_zip import CreateZip, ZipPipelineData
from stream_zip import ZIP_64

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

async def get_data():
    with open("tests/files_for_tests/10mb_zero_file.txt", 'rb') as f:
        file_content = f.read()

    chunk_size = 1 * 1024 * 1024
    total_size = len(file_content)

    for i in range(1000):
        queue = asyncio.Queue()
        index = 0
        for start in range(0, total_size, chunk_size):
            chunk = file_content[start:start + chunk_size]
            index = index + 1
            await queue.put(("chunk", chunk))
        await queue.put(('endfile', b''))
        yield QueuePipelineData(queue, {'media_type': 'text/plain', 'file_name': f'file_{i}'})

@pytest.mark.asyncio(loop_scope="session")
async def test_zip_pipeline_data_get_async_data():

    total_files = 0
    zip_pipeline_data = ZipPipelineData(input_streams=None, metadata={})
    async for input_stream in get_data():
        total_files = total_files + 1
        print(f'current file:{total_files}')
        buffer = io.BytesIO()
        async for chunk in zip_pipeline_data.get_async_data(input_stream):
            buffer.write(chunk)
        assert len(buffer.getvalue()) == 10485760  # 10 mb

    assert total_files == 1000

@pytest.mark.asyncio(loop_scope="session")
async def test_zip_pipeline_data_async_member_files():
    input_streams = get_data()
    zip_pipeline_data = ZipPipelineData(input_streams, metadata={})

    results = []
    async for file_metadata in zip_pipeline_data.async_member_files(input_streams):
        results.append(file_metadata)

    file_names = [f'file_{i}' for i in range(1000)]

    for index, (file_name, file_date, file_mode, file_zip_flag, file_stream) in enumerate(results):
        print(f'validating file:{index}')
        assert file_name ==  file_names[index]
        assert file_mode == S_IFREG | 0o600
        assert file_zip_flag == ZIP_64

        buffer = io.BytesIO()
        async for chunk in file_stream:
            buffer.write(chunk)
        expected_content = b''
        with open("tests/files_for_tests/10mb_zero_file.txt", 'rb') as f:
            expected_content = f.read()

        assert buffer.getvalue() == expected_content

@pytest.mark.asyncio(loop_scope="session")
async def test_zip_pipeline_data_zipped_file_iterator():
    input_streams = get_data()
    zip_pipeline_data = ZipPipelineData(input_streams, metadata={})

    zipped_file_iterator = zip_pipeline_data.zipped_file_iterator

    buffer = io.BytesIO()
    async for chunk in zipped_file_iterator:
        buffer.write(chunk)

    buffer.seek(0)
    with zipfile.ZipFile(buffer, 'r') as zf:
        zip_file_names = zf.namelist()

        assert 'file_0' in zip_file_names
        assert 'file_1' in zip_file_names

        with zf.open('file_1') as f:
            zip_file_content = f.read()

        with open("tests/files_for_tests/10mb_zero_file.txt", 'rb') as f:
            original_file_content = f.read()

        assert zip_file_content == original_file_content, "The content of 'file_1' doesn't match the original file."

@pytest.mark.asyncio(loop_scope="session")
async def test_ZipPipelineData_zipped_file_iterator_while_loop():
    input_streams = get_data()
    zip_pipeline_data = ZipPipelineData(input_streams, metadata={})

    zipped_file_iterator = zip_pipeline_data.zipped_file_iterator

    buffer = io.BytesIO()
    try:
        while True:
            chunk = await anext(zipped_file_iterator)
            buffer.write(chunk)
    except StopAsyncIteration:
        pass

    buffer.seek(0)
    with zipfile.ZipFile(buffer, 'r') as zf:
        zip_file_names = zf.namelist()

        print("Files inside the zip:", zip_file_names)

        assert 'file_0' in zip_file_names
        assert 'file_1' in zip_file_names

        with zf.open('file_1') as f:
            zip_file_content = f.read()

        with open("tests/files_for_tests/10mb_zero_file.txt", 'rb') as f:
            original_file_content = f.read()

        assert zip_file_content == original_file_content, "The content of 'file_1' doesn't match the original file."

@pytest.mark.asyncio(loop_scope="session")
async def test_zip_pipeline_data_anext():
    input_streams = get_data()
    zip_pipeline_data = ZipPipelineData(input_streams, metadata={})

    chunk_1 = await anext(zip_pipeline_data)
    assert chunk_1 is not None
    assert len(chunk_1) > 0

    chunk_2 = await anext(zip_pipeline_data)
    assert chunk_2 is not None
    assert len(chunk_2) > 0

    assert chunk_1 != chunk_2

@pytest.mark.asyncio(loop_scope="session")
async def test_zip_pipeline_data_anext_for_loop():
    input_streams = get_data()
    zip_pipeline_data = ZipPipelineData(input_streams, metadata={})

    buffer = io.BytesIO()
    async for chunk in zip_pipeline_data:
        buffer.write(chunk)

    buffer.seek(0)
    with zipfile.ZipFile(buffer, 'r') as zf:
        zip_file_names = zf.namelist()


        assert 'file_0' in zip_file_names
        assert 'file_1' in zip_file_names

        with zf.open('file_1') as f:
            zip_file_content = f.read()

        with open("tests/files_for_tests/10mb_zero_file.txt", 'rb') as f:
            original_file_content = f.read()

        assert zip_file_content == original_file_content, "The content of 'file_1' doesn't match the original file."

@pytest.mark.asyncio(loop_scope="session")
async def test_create_zip_success_from_input_bytes_1000_files():
    inputs = get_data()
    create_zip = CreateZip()
    result = create_zip.process(inputs, {})
    zip_pipeline_data = await anext(result)
    assert zip_pipeline_data.metadata['file_name'] == "created.zip"

    buffer = io.BytesIO()

    async for chunk in zip_pipeline_data:
        buffer.write(chunk)

    with zipfile.ZipFile(buffer, 'r') as zip_file:
        namelist = zip_file.namelist()

        zip_file_name =namelist[0]
        assert zip_file_name == 'file_0'
        with zip_file.open(zip_file_name) as file_in_zip:
            zip_content = file_in_zip.read()
            with open("tests/files_for_tests/10mb_zero_file.txt", 'rb') as original_file:
                original_content = original_file.read()
                assert zip_content == original_content


