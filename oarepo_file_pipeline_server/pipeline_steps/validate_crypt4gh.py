#
# Copyright (C) 2025 CESNET z.s.p.o.
#
# oarepo-file-pipeline-server is free software; you can redistribute it and/or
# modify it under the terms of the MIT License; see LICENSE file for more
# details.
#
"""Pipeline step for validating Crypt4GH encrypted files."""

from __future__ import annotations

import json
from io import BytesIO

from oarepo_c4gh import Crypt4GH  # type: ignore[import-untyped]

from oarepo_file_pipeline_server.config import REPOSITORY_CRYPT4GH_KEY_COLLECTION
from oarepo_file_pipeline_server.pipeline_data.base import PipelineData
from oarepo_file_pipeline_server.pipeline_data.url_pipeline_data import UrlPipelineData
from oarepo_file_pipeline_server.pipeline_steps.base import PipelineStep, StepIO


class Crypt4GHValidateStep(PipelineStep):
    """Pipeline step to validate if a Crypt4GH file can be decrypted.

    This step checks if the server can decrypt the file using the configured
    repository keys. Useful for validating user uploads before processing.
    """

    def process(self, inputs: StepIO, args: dict) -> StepIO:
        """Validate if a Crypt4GH file can be decrypted.

        :param inputs: StepIO object.
        :param args: A dictionary of additional arguments (e.g. source_url).
        :return: StepIO containing validation result in metadata.
        :raises ValueError: If no input data or source_url is provided.
        """
        if not inputs and not args:
            raise ValueError("No input data or arguments were provided to Crypt4GH validation step.")

        # Get input stream
        if inputs:
            input_stream = inputs[0]
        elif args and "source_url" in args:
            input_stream = UrlPipelineData(args["source_url"])
        else:
            raise ValueError("No input nor source_url were provided.")

        # Validate the file
        validation_result = self._validate_crypt4gh(input_stream)

        # Create JSON response with validation result
        response_data = {
            "valid": validation_result["valid"],
            "error": validation_result.get("error"),
            "file_name": input_stream.metadata.get("file_name", "unknown"),
        }
        json_response = json.dumps(response_data, indent=2).encode("utf-8")

        # Return JSON in the stream
        # BytesIO is compatible with RawIOBase for reading purposes
        result_data = PipelineData(
            stream=BytesIO(json_response),  # type: ignore[arg-type]
            metadata={
                "media_type": "application/json",
                "file_name": "validation_result.json",
                "validation": validation_result["valid"],
                "error": validation_result.get("error"),
                "download": False,
            },
        )

        return StepIO([result_data])

    def _validate_crypt4gh(self, input_stream: PipelineData) -> dict:
        """Validate if a Crypt4GH file can be decrypted.

        :param input_stream: The input PipelineData containing the Crypt4GH file.
        :return: Dictionary with 'valid' (bool) and optional 'error' (str) keys.
        """
        try:
            # Try to open and read the entire file incrementally to validate full decryption
            crypt4gh = Crypt4GH(reader_key=REPOSITORY_CRYPT4GH_KEY_COLLECTION, istream=input_stream.stream)
            opened_stream = crypt4gh.open("rb")

            # Read through the entire file in chunks without storing in memory
            # This ensures the entire file can be decrypted successfully
            chunk_size = 65536  # 64KB chunks
            while True:
                chunk = opened_stream.read(chunk_size)
                if not chunk:
                    break
                # Chunk is read but immediately discarded (not stored)

            # Close the stream
            if hasattr(opened_stream, "close"):
                opened_stream.close()

            # If we got here, the entire file is valid and can be decrypted
            return {  # noqa: TRY300
                "valid": True,
                "error": None,
            }

        except Exception as e:  # noqa: BLE001
            # If any error occurs, the file cannot be decrypted
            return {
                "valid": False,
                "error": str(e),
            }
