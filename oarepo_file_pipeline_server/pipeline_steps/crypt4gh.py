#
# Copyright (C) 2025 CESNET z.s.p.o.
#
# oarepo-file-pipeline-server is free software; you can redistribute it and/or
# modify it under the terms of the MIT License; see LICENSE file for more
# details.
#
"""Crypt4GH Step."""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

from oarepo_c4gh import C4GHKey
from oarepo_c4gh import Crypt4GH as Crypt4GHReader

from oarepo_file_pipeline_server.config import server_key_priv_c4gh
from oarepo_file_pipeline_server.pipeline_data.base import PipelineData
from oarepo_file_pipeline_server.pipeline_data.url_pipeline_data import UrlPipelineData
from oarepo_file_pipeline_server.pipeline_steps.base import PipelineStep, StepResults

if TYPE_CHECKING:
    from collections.abc import Iterator
    from io import RawIOBase


class Crypt4GHReaderWrapper(PipelineData):
    """Wrapper around Crypt4GHReader to implement PipelineData protocol."""

    def __init__(self, crypt4gh_reader: Crypt4GHReader, metadata: dict | None = None) -> None:
        """Initialize the Crypt4GHReaderWrapper with a Crypt4GHReader.

        :param crypt4gh_reader: The Crypt4GH reader instance
        :param metadata: Optional metadata dictionary
        """
        self._crypt4gh_reader = crypt4gh_reader
        self._data_blocks_iterator = iter(self._crypt4gh_reader.data_blocks)
        self._buffer = b""  # Buffer for partial reads
        self._metadata = metadata or {
            "media_type": "application/octet-stream",
            "file_name": "decrypted_output",
        }

    def read_all(self) -> bytes:
        """Read all decrypted data from the Crypt4GHReader."""
        chunks = []

        # Include any buffered data first
        if self._buffer:
            chunks.append(self._buffer)
            self._buffer = b""

        # Extend chunks with all non-empty cleartext blocks
        chunks.extend(data_block.cleartext for data_block in self._data_blocks_iterator if data_block.cleartext)

        return b"".join(chunks) if chunks else b""

    def read(self, n: int = -1) -> bytes:  # noqa: PLR0911
        """Read decrypted data from the Crypt4GHReader.

        :param n: Number of bytes to read. If negative, read all available data.
        :return: Decrypted data as bytes.

        Note: For streaming large files, use iteration (for chunk in obj) instead of read(-1)
        to avoid loading all data into memory.
        """
        # Handle negative values (read all) - loads all data into memory
        if n < 0:
            return self.read_all()

        # Handle zero bytes request
        if n == 0:
            return b""

        # First, check if buffer has enough data
        if self._buffer and len(self._buffer) >= n:
            # Buffer has enough data - return immediately
            result = self._buffer[:n]
            self._buffer = self._buffer[n:]
            return result

        # Not enough data in buffer - need to read more blocks
        # Try to get next data block
        try:
            data_block = next(self._data_blocks_iterator)
        except StopIteration:
            # No more blocks, return whatever is in buffer
            result = self._buffer
            self._buffer = b""
            return result

        if not data_block.cleartext:
            # Empty block, return whatever is in buffer
            result = self._buffer
            self._buffer = b""
            return result

        block_data = data_block.cleartext

        # If buffer is empty and block has enough data, return directly
        if not self._buffer:
            if len(block_data) >= n:
                # Block alone is enough - return requested amount directly
                self._buffer = block_data[n:]
                return cast("bytes", block_data[:n])
            # Block is not enough, buffer it and try to get more
            self._buffer = block_data
            return self.read(n)  # Recursive call to continue reading
        # Combine buffer with new block
        combined = self._buffer + block_data
        self._buffer = combined[n:]
        return cast("bytes", combined[:n])

    def __iter__(self):
        """Return self for iteration."""
        return self

    def __next__(self) -> bytes:
        """Get next chunk of data (65000 bytes).

        :return: Next chunk of decrypted data
        :raises StopIteration: When no more data available
        """
        chunk = self.read(65000)
        if not chunk:
            raise StopIteration
        return chunk

    @property
    def metadata(self) -> dict:
        """Property holding metadata like file_name, media_type, etc.

        :return: The metadata dictionary.
        """
        return self._metadata

    @metadata.setter
    def metadata(self, value: dict) -> None:
        """Setter for updating metadata.

        :param value: A dictionary with new metadata values.
        """
        self._metadata.update(value)


class Crypt4GHDecryptStep(PipelineStep):
    """Pipeline step to decrypt a Crypt4GH file."""

    def process(self, inputs: Iterator[PipelineData] | None, args: dict) -> StepResults:
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
            input_stream = next(inputs)
        elif args and "source_url" in args:
            input_stream = UrlPipelineData(args["source_url"])
        else:
            raise ValueError("No input nor source_url were provided.")

        # Process the file (decrypt it)
        output_data = self._process_crypt4gh(input_stream)

        # Return results as a single-file iterator
        def get_results() -> Iterator[PipelineData]:
            yield output_data

        return StepResults(file_count=1, results=get_results())

    def _process_crypt4gh(self, input_stream: PipelineData) -> PipelineData:
        """Decrypt a Crypt4GH file.

        :param input_stream: The input PipelineData containing the Crypt4GH file.
        :return: A PipelineData containing the decrypted data (streaming).
        """
        # Load server key
        server_key = C4GHKey.from_string(server_key_priv_c4gh)

        # Create Crypt4GH reader - it will decrypt the data
        crypt4gh = Crypt4GHReader(reader_key=server_key, istream=cast("RawIOBase", input_stream))

        # Create metadata
        input_filename = input_stream.metadata.get("file_name", "output.c4gh")
        # Remove .c4gh extension if present
        output_filename = input_filename[:-5] if input_filename.endswith(".c4gh") else input_filename + ".decrypted"

        metadata = {
            "media_type": "application/octet-stream",
            "file_name": output_filename,
        }

        # Return wrapped reader for streaming
        return Crypt4GHReaderWrapper(crypt4gh, metadata=metadata)
