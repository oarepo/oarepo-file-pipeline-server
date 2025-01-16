import io
import unittest

import aiohttp
import pytest

from oarepo_file_pipeline_server.pipeline_data.bytes_pipeline_data import BytesPipelineData
from oarepo_file_pipeline_server.pipeline_data.queue_pipeline_data import QueuePipelineData
from oarepo_file_pipeline_server.pipeline_data.url_pipeline_data import UrlPipelineData
from oarepo_file_pipeline_server.pipeline_steps.extract_file_zip import ExtractFileZip


class TestExtractFileZip(unittest.IsolatedAsyncioTestCase):
    async def test_extract_file_zip_success_from_url(self):
        step = ExtractFileZip()

        outputs = step.process(None,
                args={
                    "source_url": "https://github.com/oarepo/oarepo-file-pipeline-server/raw/refs/heads/first-version/tests/test_zip.zip",
                    "file_name": "test_zip/test1.txt"
                })

        output = await anext(outputs)

        assert output.metadata['file_name'] == 'test_zip/test1.txt'
        assert output.metadata['media_type'] == 'text/plain'
        buffer = io.BytesIO()
        async for content in output:
            buffer.write(content)
        assert buffer.getvalue() == b'Hello World\n'

        with self.assertRaises(StopAsyncIteration):
            await anext(outputs)


    async def test_extract_file_zip_success_from_input_bytes(self):
        with open("files_for_tests/test_zip.zip", "rb") as f:
            zip_stream = io.BytesIO(f.read())

        async def get_data():
            yield BytesPipelineData({'media_type': 'application/zip'}, zip_stream)

        inputs = get_data()

        step = ExtractFileZip()
        outputs = step.process(inputs, args={'file_name': "test_zip/test1.txt"})
        output = await anext(outputs)

        assert output.metadata['file_name'] == 'test_zip/test1.txt'
        assert output.metadata['media_type'] == 'text/plain'
        buffer = io.BytesIO()
        async for content in output:
            buffer.write(content)
        assert buffer.getvalue() == b'Hello World\n'

        with pytest.raises(StopAsyncIteration):
            await anext(outputs)


    async def test_extract_file_zip_success_from_input_url(self):
        session = aiohttp.ClientSession()
        async def get_data():
            yield UrlPipelineData("https://github.com/oarepo/oarepo-file-pipeline-server/raw/refs/heads/first-version/tests/test_zip.zip",session)

        inputs = get_data()
        step = ExtractFileZip()
        outputs = step.process(inputs,args={'file_name': "test_zip/test1.txt"})
        output = await anext(outputs)

        assert output.metadata['file_name'] == 'test_zip/test1.txt'
        assert output.metadata['media_type'] == 'text/plain'
        buffer = io.BytesIO()
        async for content in output:
            buffer.write(content)
        assert buffer.getvalue() == b'Hello World\n'

        with self.assertRaises(StopAsyncIteration):
            await anext(outputs)

    async def test_extract_file_fail_no_inputs_no_args(self):
        step = ExtractFileZip()

        with self.assertRaisesRegex(ValueError, 'No input or arguments were provided to ExtractFile step'):
            outputs = step.process(None,{})
            await anext(outputs)

    async def test_extract_file_fail_no_file_name(self):
        step = ExtractFileZip()

        outputs = step.process(None,{'source_url' : 'something'})
        with self.assertRaisesRegex(ValueError, 'No file name to extract was provided.'):
            await anext(outputs)

    async def test_extract_file_fail_non_existing_file_name(self):
        step = ExtractFileZip()

        with open("files_for_tests/test_zip.zip", "rb") as f:
            zip_stream = io.BytesIO(f.read())
        async def get_data():
            yield BytesPipelineData({'media_type': 'application/zip'}, zip_stream)

        outputs = step.process(get_data(), args={'file_name': "test_zip/test1.tx"})
        with self.assertRaisesRegex(ValueError, "File 'test_zip/test1.tx' not found in the ZIP archive."):
            await anext(outputs)

    async def test_preview_zip_invalid_inputs(self):
        step = ExtractFileZip()
        with open("files_for_tests/test_zip/test1.txt", "rb") as f:
            text_stream = io.BytesIO(f.read())
        async def get_data():
            yield BytesPipelineData({'media_type': 'application/zip'}, text_stream)

        with self.assertRaisesRegex(ValueError,"Input stream is not a valid ZIP file."):
            outputs = step.process(get_data(), args={'file_name': "test_zip/test1.txt"})
            await anext(outputs)


if __name__ == "__main__":
    unittest.main()

