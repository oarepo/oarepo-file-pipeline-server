#
# Copyright (C) 2025 CESNET z.s.p.o.
#
# oarepo-file-pipeline-server is free software; you can redistribute it and/or
# modify it under the terms of the MIT License; see LICENSE file for more
# details.
#
"""Class that represents pipeline data with data read from URL stream."""

from __future__ import annotations

import io

import requests

from .base import PipelineData


class URLStream(io.RawIOBase):
    """HTTP stream wrapper with seek support using range requests.

    This class implements a seekable stream interface over HTTP by using
    range requests. It efficiently handles chunked reading and caches the
    file size for optimal performance.
    """

    def __init__(self, url: str):
        """Initialize the URLStream with a URL.

        :param url: The URL to stream data from.
        """
        super().__init__()
        self._url = url
        self._current_reader: requests.Response | None = None
        self._response: requests.Response | None = None
        self._current_pos = 0
        self._size: int | None = None

    def read(self, n: int = -1) -> bytes:
        """Read a specific number of bytes from the URL stream.

        If the `n` is -1, it will read the entire remaining stream. The data is
        read in chunks, and the method will handle any partial reads and return
        the accumulated bytes.

        :param n: The number of bytes to read. If -1, it will read until EOF.
        :return: A chunk of data (bytes).
        """
        if not self._current_reader:
            self.seek(0)

        # Read chunk by chunk for better memory efficiency
        if n == -1:
            # Read all remaining data
            ret = io.BytesIO()
            for chunk in self._current_reader.iter_content(chunk_size=65000):  # type: ignore[attr-defined, union-attr]
                if not chunk:
                    break
                self._current_pos += len(chunk)
                ret.write(chunk)
            return ret.getvalue()
        # Read specific size
        ret = io.BytesIO()
        remaining = n

        # At the end of file, return empty bytes
        if self._current_pos >= self.get_size():  # pragma: no cover
            return b""

        for chunk in self._current_reader.iter_content(chunk_size=min(remaining, 65000)):  # type: ignore[attr-defined, union-attr]
            if not chunk:
                break
            self._current_pos += len(chunk)
            ret.write(chunk)
            remaining -= len(chunk)
            if remaining <= 0:
                break
        return ret.getvalue()

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        """Seek to a specific position in the stream.

        This method allows moving to a certain byte position in the stream.
        It supports different modes for the offset, such as absolute or relative.

        :param offset: The byte position to seek to.
        :param whence: Specifies how the offset is interpreted. Default is 0 (absolute).
        :return: The new absolute position.
        """
        if whence == io.SEEK_END and offset == 0:
            self._current_pos = self.get_size()
            self._current_reader = None
            return self._current_pos

        if whence == io.SEEK_END:
            offset += self.get_size()
        elif whence == io.SEEK_CUR:
            offset += self._current_pos
        else:
            pass

        # optimization that stays on the same position if already there, therefore does not open another connection
        if offset == self._current_pos and self._current_reader is not None:
            return self._current_pos  # type: ignore[unreachable]

        # optimization that read some bytes instead seeking, therefore does not open another connection
        if (offset - self._current_pos > 0) and (offset - self._current_pos < 1000):  # noqa: PLR2004
            self.read(offset - self._current_pos)
            return self._current_pos

        if self._response:  # pragma: no cover
            self._response.close()  # type: ignore[unreachable]

        self._response = requests.get(
            self._url,
            headers={
                "range": f"bytes={offset}-",
                "Accept-Encoding": "identity",  # ensure file is not zipped
            },
            stream=True,
            timeout=10,
        )

        if self._response.status_code != 206:  # noqa: PLR2004
            content_range = self._response.headers.get("Content-Length", "N/A")
            raise ValueError(f"URL Source does not support seek(), offset={offset}, Content-Range={content_range}")

        self._current_reader = self._response
        self._current_pos = offset
        return self._current_pos

    def tell(self) -> int:
        """Return the current position in the stream.

        :return: The current position (in bytes) within the stream.
        """
        return self._current_pos

    def get_size(self) -> int:
        """Get the total size of the file being read from the URL.

        This method makes a request to the server to retrieve the content range
        and size of the file. It caches the size after the first request for efficiency.

        :return: The total size of the file in bytes.
        """
        if self._size is not None:
            return self._size  # type: ignore[unreachable]

        response = requests.get(self._url, headers={"range": "bytes=0-0"}, timeout=10)
        if response.status_code != 206:  # noqa: PLR2004
            raise ValueError(f"Failed to fetch file from URL: {response.status_code}")
        self._size = int(response.headers.get("Content-Range", "0/0").split("/")[-1])
        response.close()
        return self._size

    def close(self) -> None:
        """Close the underlying HTTP connection."""
        if self._response:
            self._response.close()


class URLMetadata(dict):
    """Dictionary containing metadata fetched from HTTP headers.

    Retrieves metadata like content type and source URL from HTTP response
    headers and stores them in a dictionary format.
    """

    def __init__(self, url: str):
        """Initialize URLMetadata by fetching metadata from the URL.

        :param url: The URL to fetch metadata from.
        """
        super().__init__()
        self["source_url"] = url

        # TODO: request.head for returns forbidden on presigned URLs
        self._response = requests.get(
            url,
            headers={
                "Accept-Encoding": "identity",  # ensure file is not zipped
            },
            stream=True,
            timeout=10,
        )

        self["media_type"] = self._response.headers.get("Content-Type", "application/octet-stream")
        self._response.close()


class UrlPipelineData(PipelineData):
    """Pipeline data that reads data from a URL stream."""

    def __init__(self, url: str) -> None:
        """Initialize the UrlPipelineData object with a URL."""
        super().__init__(stream=URLStream(url), metadata=URLMetadata(url))
