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

import os

from oarepo_file_pipeline_server.key_manager_service import KeyManagementService

STEP_DEFINITIONS = {
    "decrypt_crypt4gh": ("oarepo_file_pipeline_server.pipeline_steps.decrypt_crypt4gh.Crypt4GHDecryptStep"),
    "add_recipient_crypt4gh": (
        "oarepo_file_pipeline_server.pipeline_steps.add_recipient_crypt4gh.AddRecipientCrypt4GHStep"
    ),
    "validate_crypt4gh": ("oarepo_file_pipeline_server.pipeline_steps.validate_crypt4gh.Crypt4GHValidateStep"),
}

"""Default algorithms"""
PIPELINE_SIGNING_ALGORITHM = "RS256"
PIPELINE_ENCRYPTION_ALGORITHM = "RSA-OAEP"
PIPELINE_ENCRYPTION_METHOD = "A256GCM"

# Initialize Key configuration manager
# This loads HSM server URLs and RSA Keys from config.json
# Use CONFIG_FILE environment variable to override (e.g., for testing)
# Default to /config/config.json for Docker deployment
_config_file = os.environ.get("CONFIG_FILE", "/config/config.json")
key_management_service = KeyManagementService(_config_file)

# Expose the KeyCollection for use by pipeline steps
REPOSITORY_CRYPT4GH_KEY_COLLECTION = key_management_service.get_crypt4gh_key_collection()

# Expose the manager for dynamic configuration
KEY_MANAGEMENT_SERVICE = key_management_service
