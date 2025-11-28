#
# Copyright (C) 2025 CESNET z.s.p.o.
#
# oarepo-file-pipeline-server is free software; you can redistribute it and/or
# modify it under the terms of the MIT License; see LICENSE file for more
# details.
#
"""Pipeline step for adding recipients to Crypt4GH encrypted files."""

from __future__ import annotations

import io
from io import BytesIO
from typing import TYPE_CHECKING

from oarepo_c4gh import AddRecipientFilter, C4GHKey, Crypt4GH  # type: ignore[import-untyped]

from oarepo_file_pipeline_server.config import REPOSITORY_CRYPT4GH_KEY_COLLECTION
from oarepo_file_pipeline_server.pipeline_data.base import PipelineData
from oarepo_file_pipeline_server.pipeline_data.url_pipeline_data import UrlPipelineData
from oarepo_file_pipeline_server.pipeline_steps.base import PipelineStep, StepIO

if TYPE_CHECKING:
    from oarepo_c4gh.common.proto4gh import Proto4GH  # type: ignore[import-untyped]


class Crypt4GHReaderWrapper(io.RawIOBase):
    """Wrapper that serializes Crypt4GH header and data blocks into a stream.

    This wrapper implements the io.RawIOBase interface to provide streaming
    access to Crypt4GH encrypted data, including the header and data blocks.
    """

    def __init__(self, crypt4gh_reader: Proto4GH) -> None:
        """Initialize the wrapper with a Crypt4GH reader.

        :param crypt4gh_reader: The Crypt4GH reader containing header and data blocks.
        """
        self._buffer = BytesIO()  # Buffer for partial reads

        # We serialize the header first to the buffer since it would be relatively small
        self._buffer.write(crypt4gh_reader.header.magic_bytes)
        self._buffer.write(crypt4gh_reader.header.version.to_bytes(4, "little"))
        self._buffer.write(len(crypt4gh_reader.header.packets).to_bytes(4, "little"))
        for packet in crypt4gh_reader.header.packets:
            self._buffer.write(packet.packet_data)

        self._buffer.seek(0)

        self.data_blocks = crypt4gh_reader.data_blocks

    def readinto(self, b: bytearray) -> int:  # type: ignore[override]
        """Read data into the provided buffer.

        :param b: A buffer object to read data into.
        :return: Number of bytes read, or 0 at EOF.
        """
        num_of_bytes = self._buffer.readinto(b)

        if num_of_bytes > 0:
            return num_of_bytes

        # Need to read more data from data blocks
        try:
            next_block = next(iter(self.data_blocks))
        except StopIteration:
            return 0  # EOF

        self._buffer.truncate(0)
        self._buffer.seek(0)
        self._buffer.write(next_block.ciphertext)

        self._buffer.seek(0)
        return self._buffer.readinto(b)


class AddRecipientCrypt4GHStep(PipelineStep):
    """Pipeline step to add recipients to a Crypt4GH encrypted file.

    This step allows additional recipients to decrypt the file by adding their
    public keys to the Crypt4GH header without re-encrypting the entire file.
    """

    def process(self, inputs: StepIO, args: dict) -> StepIO:
        """Add a recipient to a Crypt4GH encrypted file.

        :param inputs: List of PipelineData objects containing the encrypted file.
        :param args: A dictionary of additional arguments (e.g. source_url, recipient_key).
        :return: StepIO containing the file with added recipient.
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

        recipient_pub = args.get("recipient_pub")
        if not recipient_pub:
            raise ValueError("No recipient_pub provided in arguments.")

        # Process the file
        output_data = self._process_crypt4gh(input_stream, recipient_pub)

        return StepIO([output_data])

    def _process_crypt4gh(self, input_stream: PipelineData, recipient_pub: str) -> PipelineData:
        """Add a recipient to a Crypt4GH encrypted file.

        :param input_stream: The input PipelineData containing the encrypted Crypt4GH file.
        :return: A PipelineData containing the file with the added recipient (streaming).
        """
        # Load recipient key to add to the Crypt4GH header
        recipient_pub_key = C4GHKey.from_string(recipient_pub)

        # Create Crypt4GH reader
        crypt4gh = Crypt4GH(reader_key=REPOSITORY_CRYPT4GH_KEY_COLLECTION, istream=input_stream.stream)
        filter4gh = AddRecipientFilter(crypt4gh, recipient_pub_key.public_key)  # type: ignore[arg-type]

        return PipelineData(
            stream=Crypt4GHReaderWrapper(filter4gh),
            metadata={
                "file_name": input_stream.metadata.get("file_name", "output.c4gh"),
                "media_type": "application/octet-stream",
                "download": True,
            },
        )
