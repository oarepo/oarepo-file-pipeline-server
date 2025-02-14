import asyncio
import io
import random
import unittest
import aiohttp
import pytest

from oarepo_file_pipeline_server.pipeline_data.bytes_pipeline_data import BytesPipelineData
from oarepo_file_pipeline_server.pipeline_data.queue_pipeline_data import QueuePipelineData
from oarepo_file_pipeline_server.pipeline_data.url_pipeline_data import UrlPipelineData
from collections import namedtuple

@pytest.mark.asyncio(loop_scope="session")
async def test_async_test_aiter():
    data = BytesPipelineData({'file_name':'neco'}, io.BytesIO(b'123neco456neco'))

    buffer = io.BytesIO()
    async for byte in data:
        buffer.write(byte)

    assert buffer.getvalue() == b'123neco456neco'
    assert data.metadata == {'file_name': 'neco'}



@pytest.mark.asyncio(loop_scope="session")
async def test_bytes_pipeline_data_constructor():
    with pytest.raises(ValueError,match="Stream must be an instance of io.IOBase."):
         BytesPipelineData({'file_name':'neco'}, str('123neco456neco'))

@pytest.mark.asyncio(loop_scope="session")
async def test_bytes_pipeline_data_seek():
    data = BytesPipelineData({'file_name':'neco'}, io.BytesIO(b'123neco456neco'))

    await data.seek(0)
    assert await data.read(1) == b'1'

    await data.seek(3)
    assert await data.read(1) == b'n'

    from os import SEEK_END
    await data.seek(0, SEEK_END)
    assert await data.read(1) == b''

    await data.seek(-5, SEEK_END)
    assert await data.read(5) == b'6neco'

    pos = await data.seek(100)
    assert pos == 100



@pytest.mark.asyncio(loop_scope="session")
@pytest.mark.parametrize("chunk_size", [1, 2, 5, 10])
async def test_bytes_pipeline_data_read_chunks(chunk_size):
    data = BytesPipelineData({'file_name':'neco'}, io.BytesIO(b'123neco456neco'))

    buffer = io.BytesIO()
    while r := await data.read(chunk_size):
        buffer.write(r)

    assert buffer.getvalue() == b'123neco456neco'
    assert data.metadata == {'file_name': 'neco'}


@pytest.mark.asyncio(loop_scope="session")
async def test_bytes_pipeline_data_read_all():
    data = BytesPipelineData({'file_name':'neco'}, io.BytesIO(b'123neco456neco'))

    r = await data.read()

    assert r == b'123neco456neco'

@pytest.mark.asyncio
async def test_url_pipeline_data_seek():
    with open('tests/files_for_tests/test_zip.zip', 'rb') as f:
        expected_data = f.read()
    buffer = io.BytesIO(expected_data)

    async with aiohttp.ClientSession() as session:
        data = UrlPipelineData(
            "https://github.com/oarepo/oarepo-file-pipeline-server/raw/refs/heads/first-version/tests/files_for_tests/test_zip.zip",
            session
        )

        assert data._size is None
        assert data._current_reader is None

        await data.seek(0, 2)
        assert data._size == len(expected_data)
        assert data._current_reader is None
        assert data._current_pos == len(expected_data)

        await data.seek(-10,2)
        assert data._size == len(expected_data)
        assert data._current_pos == len(expected_data) - 10

        buffer.seek(-10, 2)
        assert buffer.read() == await data.read()

        await data.seek(0,1)
        assert data._current_pos == len(expected_data)

@pytest.mark.asyncio
async def test_url_pipeline_data_tell():
    with open('tests/files_for_tests/test_zip.zip', 'rb') as f:
        expected_data = f.read()
    buffer = io.BytesIO(expected_data)

    async with aiohttp.ClientSession() as session:
        data = UrlPipelineData(
            "https://github.com/oarepo/oarepo-file-pipeline-server/raw/refs/heads/first-version/tests/files_for_tests/test_zip.zip",
            session
        )

        actual_data = await data.read(10)
        expected_data = buffer.read(10)
        assert actual_data == expected_data

        assert await data.tell() == 10


@pytest.mark.asyncio
async def test_url_pipeline_data_aiter():
    async with aiohttp.ClientSession() as session:
        data = UrlPipelineData(
            "https://github.com/oarepo/oarepo-file-pipeline-server/raw/refs/heads/first-version/tests/files_for_tests/test_zip.zip",
            session
        )

        buffer = io.BytesIO()
        async for byte in data:
            buffer.write(byte)

    with open("tests/files_for_tests/test_zip.zip", "rb") as f:
        expected_data = f.read()

    assert buffer.getvalue() == expected_data

@pytest.mark.asyncio(loop_scope="session")
@pytest.mark.parametrize("chunk_size", [10, 30, 50, 100])
async def test_url_pipeline_data_read_chunks(chunk_size):


    async with aiohttp.ClientSession() as session:
        data = UrlPipelineData(
            "https://github.com/oarepo/oarepo-file-pipeline-server/raw/refs/heads/first-version/tests/files_for_tests/test_zip.zip",
            session
        )

        buffer = io.BytesIO()
        while r := await data.read(chunk_size):
            buffer.write(r)

    with open("tests/files_for_tests/test_zip.zip", "rb") as f:
        expected_data = f.read()

    assert buffer.getvalue() == expected_data

@pytest.mark.asyncio(loop_scope="session")
async def test_url_pipeline_data_read_all():
    async with aiohttp.ClientSession() as session:
        data = UrlPipelineData(
            "https://github.com/oarepo/oarepo-file-pipeline-server/raw/refs/heads/first-version/tests/files_for_tests/test_zip.zip",
            session
        )

        r = await data.read()

    with open("tests/files_for_tests/test_zip.zip", "rb") as f:
        expected_data = f.read()

    assert r == expected_data

@pytest.mark.asyncio
async def test_queue_pipeline_data_aiter():
    q = asyncio.Queue()
    await q.put(('chunk', b'neco'))
    await q.put(('chunk', b'neco2'))
    await q.put(('endfile', b'EOF'))

    data = QueuePipelineData(q, {'metadata': 'neco'})

    buffer = io.BytesIO()
    async for chunk in data:
        buffer.write(chunk)

    assert buffer.getvalue() == b'neconeco2'

@pytest.mark.asyncio
@pytest.mark.parametrize("chunk_size", [1, 2, 10])
async def test_queue_pipeline_data_read_chunks(chunk_size):
    q = asyncio.Queue()
    await q.put(('chunk', b'neco'))
    await q.put(('chunk', b'neco2'))
    await q.put(('endfile', b'EOF'))

    data = QueuePipelineData(q, {'metadata': 'neco'})

    buffer = io.BytesIO()
    while content := await data.read(chunk_size):
        buffer.write(content)

    assert buffer.getvalue() == b'neconeco2'

@pytest.mark.asyncio
async def test_queue_pipeline_data_read_all():
    q = asyncio.Queue()
    await q.put(('chunk', b'neco'))
    await q.put(('chunk', b'neco2'))
    await q.put(('endfile', b'EOF'))

    data = QueuePipelineData(q, {'metadata': 'neco'})

    buffer = io.BytesIO()
    content = await data.read()

    assert content == b'neconeco2'

@pytest.mark.asyncio
async def test_url_pipeline_data_download():
    with open("tests/files_for_tests/1000_files.zip", "rb") as f:
        async with aiohttp.ClientSession() as session:
            data = UrlPipelineData('https://github.com/oarepo/oarepo-file-pipeline-server/raw/refs/heads/first-version/tests/files_for_tests/1000_files.zip', session)

            index = 0
            async for chunk in data:

                file_chunk = f.read(len(chunk))
                assert chunk == file_chunk, f'failed at {index}'
                index += len(chunk)

@pytest.mark.asyncio
async def test_url_pipeline_data_seek_random_positions():
    with open("tests/files_for_tests/file_with_ints.bin", "rb") as f:
        f.seek(0, 2)
        size = f.tell()
        f.seek(0)

        for i in range(15):
            start = random.randint(0, size)

            chunk_size = random.randint(1, 128000)
            f.seek(start, 0)
            expected_bytes = f.read(chunk_size)
            async with aiohttp.ClientSession() as session:
                data = UrlPipelineData(
                    'https://github.com/oarepo/oarepo-file-pipeline-server/raw/refs/heads/first-version/tests/files_for_tests/file_with_ints.bin',
                    session)

                await data.seek(start, 0)
                ret = await data.read(chunk_size)

                if ret != expected_bytes:
                    print(f'failed at {start=}, {chunk_size=}, expected_bytes_size={len(expected_bytes)}, ret len={len(ret)}')
                    print(ret[:50])

                assert ret == expected_bytes, f'failed at {start=}, {chunk_size=}, expected_bytes_size={len(expected_bytes)}, ret len={len(ret)}'





