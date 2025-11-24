#
# Copyright (c) 2025 CESNET z.s.p.o.
#
# This file is a part of oarepo_file_pipeline_server (see https://github.com/oarepo/oarepo-file-pipeline-server).
#
# oarepo_file_pipeline_server is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.
#
"""Helpers for oarepo-file-pipeline-server."""

from __future__ import annotations

import importlib
import os
import time
from typing import TYPE_CHECKING, Any, cast

from joserfc import jwe, jwt
from joserfc.jwk import RSAKey

if TYPE_CHECKING:
    from oarepo_file_pipeline_server.pipeline_steps.base import PipelineStep

from oarepo_file_pipeline_server.config import (
    STEP_DEFINITIONS,
)


def get_payload(jwe_token: bytes) -> dict[str, Any]:
    """Decrypts jwe token, then jws token and gets payload."""
    decoded_jwe_token = jwe_token.decode("utf-8")

    # TODO: Later will be fetched from key server
    server_private_key = RSAKey.import_key(os.environ["SERVER_PRIVATE_KEY"])
    repo_public_key = RSAKey.import_key(os.environ["REPO_PUBLIC_KEY"])

    decrypted_jwe = jwe.decrypt_compact(decoded_jwe_token, server_private_key)
    encrypted_jwt = decrypted_jwe.plaintext
    if encrypted_jwt is None:
        raise ValueError("Failed to decrypt JWE token: plaintext is None")
    claims_requests = jwt.JWTClaimsRegistry(now=int(time.time()), leeway=5)
    decrypted_jwt = jwt.decode(encrypted_jwt, repo_public_key)
    claims_requests.validate_exp(value=decrypted_jwt.claims.pop("exp"))
    claims_requests.validate_iat(value=decrypted_jwt.claims.pop("iat"))

    return cast("dict", decrypted_jwt.claims)


def get_pipeline_step_obj(name: str) -> type[PipelineStep]:
    """Get pipeline step object by name."""
    pipeline_step = STEP_DEFINITIONS.get(name, None)
    if pipeline_step is None:
        raise ValueError(f"PIPELINE_STEP {name} is not defined")

    module_name, class_name = pipeline_step.rsplit(".", 1)
    module = importlib.import_module(module_name)
    return cast("type[PipelineStep]", getattr(module, class_name))
