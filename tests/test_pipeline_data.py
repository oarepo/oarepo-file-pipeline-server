import asyncio
import io
import unittest
import json
import aiohttp

from oarepo_file_pipeline_server.pipeline_data.bytes_pipeline_data import BytesPipelineData
from oarepo_file_pipeline_server.pipeline_data.queue_pipeline_data import QueuePipelineData
from oarepo_file_pipeline_server.pipeline_data.url_pipeline_data import UrlPipelineData
from collections import namedtuple

class TestBytesPipelineData(unittest.TestCase):

    async def run_async_test_aiter(self):
        data = BytesPipelineData({'file_name':'neco'}, io.BytesIO(b'123neco456neco'))

        buffer = io.BytesIO()
        async for byte in data:
            buffer.write(byte)
        return buffer.getvalue(), data.metadata

    async def run_async_test_read_1(self, n):
        data = BytesPipelineData({'file_name':'neco'}, io.BytesIO(b'123neco456neco'))

        buffer = io.BytesIO()
        while r := await data.read(n):
            buffer.write(r)

        return buffer.getvalue(), data.metadata


    def test_iteration_aiter(self):
        result = asyncio.run(self.run_async_test_aiter())
        self.assertEqual(result, (b'123neco456neco', {'file_name': 'neco'}))

    def test_iteration_read(self):
        for i in range(1, 10):
            result = asyncio.run(self.run_async_test_read_1(i))
            self.assertEqual(result, (b'123neco456neco', {'file_name': 'neco'}))

class TestURLPipelineData(unittest.TestCase):

    async def run_async_test_aiter_url(self):
        async with aiohttp.ClientSession() as session:
            data = UrlPipelineData("https://github.com/oarepo/oarepo-file-pipeline-server/raw/refs/heads/first-version/tests/test_zip.zip",
                                   session)

            buffer = io.BytesIO()
            async for byte in data:
                buffer.write(byte)
            return buffer.getvalue()

    async def run_async_test_read_url(self, n):
        async with aiohttp.ClientSession() as session:
            data = UrlPipelineData("https://github.com/oarepo/oarepo-file-pipeline-server/raw/refs/heads/first-version/tests/test_zip.zip",
                                   session)

            buffer = io.BytesIO()
            while r := await data.read(n):
                buffer.write(r)

            return buffer.getvalue()

    def test_iteration_aiter(self):
        result = asyncio.run(self.run_async_test_aiter_url())

        zip_file_path = "files_for_tests/test_zip.zip"
        with open(zip_file_path, "rb") as f:
            zip_stream = io.BytesIO(f.read())

        self.assertEqual(result, zip_stream.getvalue())

    def test_iteration_read(self):
        zip_file_path = "files_for_tests/test_zip.zip"
        with open(zip_file_path, "rb") as f:
            zip_stream = io.BytesIO(f.read())

        for n in range(10, 111, 20):
            result = asyncio.run(self.run_async_test_read_url(n))
            self.assertEqual(result, zip_stream.getvalue())

class TestQueuePipelineData(unittest.TestCase):

    async def run_async_test_aiter_queue(self):

        q = asyncio.Queue()

        # TODO change to named tuple
        await q.put(('chunk', b'neco'))
        await q.put(('chunk', b'neco2'))
        await q.put(('endfile',b'EOF'))

        q_pipeline_data = QueuePipelineData(q, {'metadata': 'neco'})


        buffer = io.BytesIO()
        async for content in q_pipeline_data:
            buffer.write(content)
        return buffer.getvalue()

    async def run_async_test_read_queue(self, n):

        q = asyncio.Queue()

        # TODO change to named tuple
        await q.put(('chunk',b'neco'))
        await q.put(('chunk', b'neco2'))
        await q.put(('endfile', b'EOF'))

        q_pipeline_data = QueuePipelineData(q, metadata={'metadata': 'neco'})
        buffer = io.BytesIO()
        while content := await q_pipeline_data.read(n):
            buffer.write(content)

        return buffer.getvalue()

    def test_iteration_aiter(self):
        result = asyncio.run(self.run_async_test_aiter_queue())
        self.assertEqual(result, b'neconeco2')

    def test_iteration_read(self):
        for n in range(1, 30, 1):
            result = asyncio.run(self.run_async_test_read_queue(n))
            self.assertEqual(result,b'neconeco2')

    def test_iteration_read_all(self):
        result = asyncio.run(self.run_async_test_read_queue(-1))
        self.assertEqual(result, b'neconeco2')


if __name__ == "__main__":
    unittest.main()


