#
# Copyright (c) 2025 CESNET z.s.p.o.
#
# This file is a part of oarepo_file_pipeline_server (see https://github.com/oarepo/oarepo-file-pipeline-server).
#
# oarepo_file_pipeline_server is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.
#
from __future__ import annotations

import datetime
from pathlib import Path

import pytest
import redis
from joserfc import jwe, jwt
from minio import Minio

from oarepo_file_pipeline_server.config import (
    repo_private_key,
    repo_public_key,
    server_key_pub_c4gh,
    server_public_key,
)


@pytest.fixture(scope="session")
def redis_client():
    return redis.Redis(host="localhost", port=6379, decode_responses=True)


@pytest.fixture(scope="session")
def minio_client():
    return Minio(
        "localhost:9000",
        access_key="invenio",
        secret_key="invenio8",  # noqa: S106
        secure=False,
    )


@pytest.fixture
def create_c4gh_file_minio(minio_client, tmp_path):
    bucket = "default"

    # Ensure bucket exists
    if not minio_client.bucket_exists(bucket):
        minio_client.make_bucket(bucket)

    # Build the file path relative to this file location
    base_dir = Path(__file__).parent
    local_path = base_dir / "secret.txt.c4gh"

    # Upload it to MinIO
    minio_client.fput_object(
        bucket_name=bucket,
        object_name="secret.txt.c4gh",
        file_path=local_path,
    )

    # Optional: verify upload
    stat = minio_client.stat_object(bucket, "secret.txt.c4gh")
    assert stat.size > 0


@pytest.fixture
def insert_into_redis_jwk_token_with_c4gh_file(redis_client, minio_client, create_c4gh_file_minio):
    # public RSA keys of running repository for signing JWT token (we use public key to verify signature)
    PIPELINE_REPOSITORY_JWK = {
        "private_key": repo_private_key,
        "public_key": repo_public_key,
    }

    # Public RSA key of this server to encrypt JWE token with payload (that is placed into redis)
    PIPELINE_JWK = {
        "public_key": server_public_key,
    }

    protected_header = {"alg": "RSA-OAEP", "enc": "A256GCM"}

    from datetime import timedelta

    url = minio_client.presigned_get_object(
        "default",
        "secret.txt.c4gh",
        expires=timedelta(minutes=10),
    )

    source_url = url
    payload = {
        "pipeline_steps": [
            {
                "type": "crypt4gh",
                "arguments": {
                    "source_url": source_url,
                    "recipient_pub": server_key_pub_c4gh,
                },
            },
        ],
        "source_url": source_url,
    }

    header = {"alg": "RS256"}

    timestamp = datetime.datetime.now(tz=datetime.UTC).timestamp()
    claims = {"iat": timestamp, "exp": timestamp + 3000, **payload}
    signed_payload = jwt.encode(header, claims, PIPELINE_REPOSITORY_JWK["private_key"])
    encrypted_token = jwe.encrypt_compact(protected_header, signed_payload, PIPELINE_JWK["public_key"])

    redis_key = "123"
    redis_client.set(redis_key, encrypted_token)
    return redis_key
