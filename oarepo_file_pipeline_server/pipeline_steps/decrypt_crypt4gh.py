#
# Copyright (C) 2025 CESNET z.s.p.o.
#
# oarepo-file-pipeline-server is free software; you can redistribute it and/or
# modify it under the terms of the MIT License; see LICENSE file for more
# details.
#
"""Pipeline step for decrypting Crypt4GH encrypted files."""

from __future__ import annotations

from oarepo_c4gh import C4GHKey, Crypt4GH  # type: ignore[import-untyped]

from oarepo_file_pipeline_server.pipeline_data.base import PipelineData
from oarepo_file_pipeline_server.pipeline_data.url_pipeline_data import UrlPipelineData
from oarepo_file_pipeline_server.pipeline_steps.base import PipelineStep, StepIO


class Crypt4GHDecryptStep(PipelineStep):
    """Pipeline step to decrypt a Crypt4GH file."""

    def process(self, inputs: StepIO, args: dict) -> StepIO:
        """Decrypt a Crypt4GH file.

        :param inputs: An iterator over `PipelineData` objects.
        :param args: A dictionary of additional arguments (e.g. source_url).
        :return: StepResults containing file_count and iterator of processed PipelineData.
        :raises ValueError: If no input data or source_url is provided.
        """
        if not inputs and not args:
            raise ValueError("No input data or arguments were provided to Crypt4GH step.")

        # Get input stream
        if inputs:
            input_stream = inputs[0]
        elif args and "source_url" in args:
            input_stream = UrlPipelineData(args["source_url"])
        else:
            raise ValueError("No input nor source_url were provided.")

        recipient_sec = args.get("recipient_sec")
        if not recipient_sec:
            raise ValueError("No recipient_sec provided in arguments.")
        # Process the file (decrypt it)
        output_data = self._process_crypt4gh(input_stream, recipient_sec)
        return StepIO([output_data])

    def _process_crypt4gh(self, input_stream: PipelineData, recipient_sec: str) -> PipelineData:
        """Decrypt a Crypt4GH file.

        :param input_stream: The input PipelineData containing the Crypt4GH file.
        :return: A PipelineData containing the decrypted data (streaming).
        """
        # Load server key
        recipient_priv_key = C4GHKey.from_string(recipient_sec)  # TODO: later will be fetched from http key server

        # Create Crypt4GH reader - it will decrypt the data
        crypt4gh = Crypt4GH(reader_key=recipient_priv_key, istream=input_stream.stream)
        opened_stream = crypt4gh.open("rb")

        # Create metadata
        input_filename = input_stream.metadata.get("file_name", "output.c4gh")
        # Remove .c4gh extension if present
        output_filename = input_filename[:-5] if input_filename.endswith(".c4gh") else input_filename + ".decrypted"

        metadata = {
            "media_type": "application/octet-stream",
            "file_name": output_filename,
        }

        return PipelineData(stream=opened_stream, metadata=metadata)
