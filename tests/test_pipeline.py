#
# Copyright (c) 2025 CESNET z.s.p.o.
#
# This file is a part of oarepo_file_pipeline_server (see https://github.com/oarepo/oarepo-file-pipeline-server).
#
# oarepo_file_pipeline_server is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.
#
from __future__ import annotations

"""Test script for FilePipelineServer.

This script tests the pipeline processing locally without running the WSGI server.
Make sure to run tmp.py first to insert a test token into Redis.
"""

import io

from oarepo_file_pipeline_server.main import FilePipelineServer


def test_process_pipeline(insert_into_redis_jwk_token_with_c4gh_file):
    TOKEN_ID = insert_into_redis_jwk_token_with_c4gh_file
    # Create server instance
    server = FilePipelineServer(redis_host="localhost", redis_port=6379, redis_db=0, prefix="pipeline")

    output_file = server.process_pipeline(TOKEN_ID)

    # Test iteration
    buffer = io.BytesIO()
    for chunk in output_file:
        buffer.write(chunk)

    assert buffer.getvalue() == b"Super super secret file", "Decrypted content does not match expected."
