import io
import unittest

import aiohttp
import pytest

from oarepo_file_pipeline_server.pipeline_data.bytes_pipeline_data import BytesPipelineData
from oarepo_file_pipeline_server.pipeline_data.url_pipeline_data import UrlPipelineData
from oarepo_file_pipeline_server.pipeline_steps.extract_directory_zip import ExtractDirectoryZip


class TestExtractDirectoryZip(unittest.IsolatedAsyncioTestCase):
    async def test_extract_directory_zip_success_from_url(self):
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
                           'file_name': 'test_directory_zip/directory1/directory1-file1.txt',
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
                           'file_name': 'test_directory_zip/directory1/directory1-file2.txt',
                           'media_type': 'text/plain'
                       } == content
            elif content == b'':
                break
            else:
                buffer.write(content)
        assert buffer.getvalue() == b'directory1-file2\n'

        with pytest.raises(StopAsyncIteration):
            await anext(outputs)


    async def test_extract_file_zip_success_from_input_bytes(self):
        async def get_data():
            with open("files_for_tests/test_directory_zip.zip", "rb") as f:
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

        with self.assertRaises(StopAsyncIteration):
            await anext(outputs)

    async def test_extract_file_zip_success_from_input_url(self):
        async def get_data():
            session = aiohttp.ClientSession()
            yield UrlPipelineData('https://github.com/oarepo/oarepo-file-pipeline-server/raw/refs/heads/first-version/tests/files_for_tests/test_directory_zip.zip', session)

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

        with self.assertRaises(StopAsyncIteration):
            await anext(outputs)


if __name__ == "__main__":
    unittest.main()

