#
# Copyright (c) 2025 CESNET z.s.p.o.
#
# This file is a part of oarepo_file_pipeline_server (see https://github.com/oarepo/oarepo-file-pipeline-server).
#
# oarepo_file_pipeline_server is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.
#
"""Configuration for the Oarepo File Pipeline Server."""

from __future__ import annotations

STEP_DEFINITIONS = {
    "decrypt_crypt4gh": ("oarepo_file_pipeline_server.pipeline_steps.decrypt_crypt4gh.Crypt4GHDecryptStep"),
    "add_recipient_crypt4gh": (
        "oarepo_file_pipeline_server.pipeline_steps.add_recipient_crypt4gh.AddRecipientCrypt4GHStep"
    ),
}

"""Default algorithms"""
PIPELINE_SIGNING_ALGORITHM = "RS256"
PIPELINE_ENCRYPTION_ALGORITHM = "RSA-OAEP"
PIPELINE_ENCRYPTION_METHOD = "A256GCM"
