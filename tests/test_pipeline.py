#
# Copyright (c) 2025 CESNET z.s.p.o.
#
# This file is a part of oarepo_file_pipeline_server (see https://github.com/oarepo/oarepo-file-pipeline-server).
#
# oarepo_file_pipeline_server is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.
#
from __future__ import annotations

import subprocess

"""Test script for FilePipelineServer.

This script tests the pipeline processing locally without running the WSGI server.
Make sure to run tmp.py first to insert a test token into Redis.
"""

import io
from pathlib import Path

from crypt4gh.header import decrypt as c4gh_decrypt

from oarepo_file_pipeline_server.main import FilePipelineServer


def load_private_key(path, passphrase=None):
    """Load a Crypt4GH secret key file."""
    with Path(path).open("rb") as f:
        sk_data = f.read()
    return c4gh_decrypt.load_secret_key(sk_data, passphrase)


def test_process_pipeline_decrypt(insert_into_redis_decrypt_step):
    TOKEN_ID = insert_into_redis_decrypt_step
    # Create server instance
    server = FilePipelineServer(redis_host="localhost", redis_port=6379, redis_db=0, prefix="pipeline")

    output_file = server.process_pipeline(TOKEN_ID)

    output_file_stream = output_file.stream
    # Test iteration
    buffer = io.BytesIO()
    for chunk in output_file_stream:
        buffer.write(chunk)

    assert buffer.getvalue() == b"Super super secret file", "Decrypted content does not match expected."


def test_process_pipeline_add_recipient(insert_into_redis_add_recipient_step):
    with Path("tests/secret.txt.c4gh").open("rb") as f:
        data = f.read()

    # Can decrypt with original key
    result = subprocess.run(
        ["crypt4gh", "decrypt", "--sk", "tests/mykey.sec"],  # noqa: S607
        check=False,
        input=data,
        capture_output=True,
    )

    assert result.stdout == b"Super super secret file"
    assert result.returncode == 0

    # Cannot decrypt with another key
    result = subprocess.run(
        ["crypt4gh", "decrypt", "--sk", "tests/myanotherkey.sec"],  # noqa: S607
        check=False,
        input=data,
        capture_output=True,
    )
    assert result.returncode != 0

    TOKEN_ID = insert_into_redis_add_recipient_step
    # Create server instance
    server = FilePipelineServer(redis_host="localhost", redis_port=6379, redis_db=0, prefix="pipeline")

    output_file = server.process_pipeline(TOKEN_ID)

    output_file_stream = output_file.stream
    # Test iteration
    buffer = io.BytesIO()
    for chunk in output_file_stream:
        buffer.write(chunk)

    # Can decrypt with both keys now
    result1 = subprocess.run(
        ["crypt4gh", "decrypt", "--sk", "tests/mykey.sec"],  # noqa: S607
        input=buffer.getvalue(),
        capture_output=True,
        check=False,
    )

    result2 = subprocess.run(
        ["crypt4gh", "decrypt", "--sk", "tests/myanotherkey.sec"],  # noqa: S607
        input=buffer.getvalue(),
        capture_output=True,
        check=False,
    )

    assert result1.stdout == result2.stdout == b"Super super secret file", (
        "Decrypted content with newly added recipient does not match expected."
    )


def test_process_pipeline_add_recipient_then_decrypt(insert_into_redis_add_recipient_then_decrypt_step):
    with Path("tests/secret.txt.c4gh").open("rb") as f:
        data = f.read()
    # Can decrypt with original key
    result = subprocess.run(
        ["crypt4gh", "decrypt", "--sk", "tests/mykey.sec"],  # noqa: S607
        check=False,
        input=data,
        capture_output=True,
    )

    assert result.stdout == b"Super super secret file"
    assert result.returncode == 0

    # Cannot decrypt with another key
    result = subprocess.run(
        ["crypt4gh", "decrypt", "--sk", "tests/myanotherkey.sec"],  # noqa: S607
        check=False,
        input=data,
        capture_output=True,
    )
    assert result.returncode != 0

    TOKEN_ID = insert_into_redis_add_recipient_then_decrypt_step
    # Create server instance
    server = FilePipelineServer(redis_host="localhost", redis_port=6379, redis_db=0, prefix="pipeline")

    # Add another recipient in pipeline, then decrypt with that recipient
    output_file = server.process_pipeline(TOKEN_ID)

    output_file_stream = output_file.stream
    # Test iteration
    buffer = io.BytesIO()
    for chunk in output_file_stream:
        buffer.write(chunk)

    assert buffer.getvalue() == b"Super super secret file", "Decrypted content does not match expected."
