import unittest

import aiohttp
import crypt4gh
import io

import pytest

from oarepo_file_pipeline_server.pipeline_data.bytes_pipeline_data import BytesPipelineData
from oarepo_file_pipeline_server.pipeline_data.url_pipeline_data import UrlPipelineData
from oarepo_file_pipeline_server.pipeline_steps.crypt4gh import Crypt4GH
import subprocess


@pytest.mark.asyncio
async def test_crypt4gh_success_from_input_bytes():
    with open("tests/files_for_tests/hello.txt.c4gh", 'rb') as f:
        data = f.read()

    async def get_data():
        yield BytesPipelineData({'media_type': 'application/octet-stream'}, io.BytesIO(data))

    result = subprocess.run(
        ["crypt4gh", "decrypt", "--sk",
         "tests/files_for_tests/server.sec"],
        input=data,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )

    assert result.stdout == b"hello world\n"
    assert result.returncode == 0

    result = subprocess.run(
        ["crypt4gh", "decrypt", "--sk",
         "tests/files_for_tests/another_researcher.sec"],
        input=data,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )

    assert result.returncode != 0

    inputs = get_data()
    step = Crypt4GH()

    with open("tests/files_for_tests/another_researcher.pub",
              'r') as key_file:
        key = key_file.read()

    outputs = step.process(inputs, args={'recipient_pub': key})
    output = await anext(outputs)
    buffer = await output.read()

    result = subprocess.run(
        ["crypt4gh", "decrypt", "--sk",
         "tests/files_for_tests/another_researcher.sec"],
        input=buffer,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )

    assert result.stdout == b"hello world\n"
    assert result.returncode == 0


@pytest.mark.asyncio
async def test_crypt4gh_success_from_inputs_url():
    async def get_data(session):
        yield UrlPipelineData(
            'https://github.com/oarepo/oarepo-file-pipeline-server/raw/refs/heads/first-version/tests/files_for_tests/hello.txt.c4gh',
            session)

    async with aiohttp.ClientSession() as session:
        data = await anext(get_data(session))
        data = await data.read()

    result = subprocess.run(
        ["crypt4gh", "decrypt", "--sk",
         "tests/files_for_tests/server.sec"],
        input=data,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )

    assert result.stdout == b"hello world\n"
    assert result.returncode == 0

    result = subprocess.run(
        ["crypt4gh", "decrypt", "--sk",
         "tests/files_for_tests/another_researcher.sec"],
        input=data,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )

    assert result.returncode != 0

    async with aiohttp.ClientSession() as session:
        inputs = get_data(session)
        step = Crypt4GH()

        with open("tests/files_for_tests/another_researcher.pub",
                  'r') as key_file:
            key = key_file.read()

        outputs = step.process(inputs, args={'recipient_pub': key})
        output = await anext(outputs)
        buffer = await output.read()

        result = subprocess.run(
            ["crypt4gh", "decrypt", "--sk",
             "tests/files_for_tests/another_researcher.sec"],
            input=buffer,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )

        assert result.returncode == 0
        assert result.stdout == b"hello world\n"


@pytest.mark.asyncio
async def test_crypt4gh_success_from_url():
    with open("tests/files_for_tests/hello.txt.c4gh", 'rb') as f:
        data = f.read()

    result = subprocess.run(
        ["crypt4gh", "decrypt", "--sk",
         "tests/files_for_tests/server.sec"],
        input=data,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )

    assert result.stdout == b"hello world\n"
    assert result.returncode == 0

    result = subprocess.run(
        ["crypt4gh", "decrypt", "--sk",
         "tests/files_for_tests/another_researcher.sec"],
        input=data,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )

    assert result.returncode != 0


    step = Crypt4GH()
    with open("tests/files_for_tests/another_researcher.pub",
              'r') as key_file:
        key = key_file.read()

    outputs = step.process(None, args={'recipient_pub': key, 'source_url':'https://github.com/oarepo/oarepo-file-pipeline-server/raw/refs/heads/first-version/tests/files_for_tests/hello.txt.c4gh'})
    output = await anext(outputs)
    buffer = await output.read()

    result = subprocess.run(
        ["crypt4gh", "decrypt", "--sk",
         "tests/files_for_tests/another_researcher.sec"],
        input=buffer,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )

    assert result.returncode == 0
    assert result.stdout == b"hello world\n"


@pytest.mark.asyncio
async def test_crypt4gh_fail_no_input_no_args():
    step = Crypt4GH()
    outputs = step.process(None, args={})
    with pytest.raises(ValueError, match="No input data or arguments were provided to Crypt4GH step."):
        output = await anext(outputs)
        buffer = await output.read()

@pytest.mark.asyncio
async def test_crypt4gh_fail_no_source_url():
    step = Crypt4GH()
    outputs = step.process(None, args={'recipient_pub': "something"})
    with pytest.raises(ValueError, match="No input nor source_url were provided."):
        output = await anext(outputs)
        buffer = await output.read()

@pytest.mark.asyncio
async def test_crypt4gh_fail_no_recipient_pub():
    step = Crypt4GH()
    outputs = step.process(None, args={'source_url':"something", 'recipient_pub': ""})
    with pytest.raises(ValueError, match="No recipient public key was provided."):
        output = await anext(outputs)
        buffer = await output.read()



