import asyncio
import io
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



