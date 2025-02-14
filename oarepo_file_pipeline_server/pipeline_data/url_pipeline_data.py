#
# Copyright (C) 2025 CESNET z.s.p.o.
#
# oarepo-file-pipeline-server is free software; you can redistribute it and/or
# modify it under the terms of the MIT License; see LICENSE file for more
# details.
#
"""
Class that represents pipeline data with data read from URL stream
"""
from __future__ import annotations

import io
from typing import TYPE_CHECKING, Self

if TYPE_CHECKING: # pragma: no cover
    import aiohttp

from oarepo_file_pipeline_server.pipeline_data.pipeline_data import PipelineData


class UrlPipelineData(PipelineData):
    def __init__(self, url: str, session: aiohttp.ClientSession) -> None:
        """Initialize the UrlPipelineData object with a URL and an aiohttp ClientSession."""
        self._url = url
        self._current_reader = None
        self._response = None
        self._current_pos = 0
        self._size = None
        self._session = session
        self._metadata = dict()

    def __aiter__(self) -> Self:
        """
        Prepare the object for asynchronous iteration.

        This method makes the object iterable in an asynchronous loop (e.g.,
        `async for` loop).

        :return: The UrlPipelineData instance itself.
        """
        return self

    async def __anext__(self) -> bytes:
        """
        Asynchronously retrieve the next chunk of data from the URL stream.

        This method is used for asynchronous iteration, retrieving chunks of
        data as specified by the `read()` method.

        :return: A chunk of data (65000 bytes).
        :raises StopAsyncIteration: If there is no more data to read.
        """
        chunk =  await self.read(65000)

        if not chunk:
            raise StopAsyncIteration
        return chunk

    async def read(self, size=-1) -> bytes:
        """
        Asynchronously read a specific number of bytes from the URL stream.

        If the `size` is -1, it will read the entire remaining stream. The data is
        read in chunks, and the method will handle any partial reads and return
        the accumulated bytes.

        :param size: The number of bytes to read. If -1, it will read until EOF.
        :return: A chunk of data (bytes).
        """
        if not self._current_reader:
            await self.seek(0)

        chunk = await self._current_reader.read(size)
        if not chunk:
            return b''

        self._current_pos += len(chunk)
        ret = io.BytesIO()
        ret.write(chunk)

        remaining = size - len(chunk)
        while remaining:
            chunk = await self._current_reader.read(remaining)
            if not chunk:
                break
            self._current_pos += len(chunk)

            remaining -= len(chunk)
            ret.write(chunk)

        return ret.getvalue()

    async def seek(self, offset, whence=0) -> None:
        """
        Seek to a specific position in the stream.

        This method allows moving to a certain byte position in the stream.
        It supports different modes for the offset, such as absolute or relative.

        :param offset: The byte position to seek to.
        :param whence: Specifies how the offset is interpreted. Default is 0 (absolute).
        """
        if whence==2 and offset==0:
            self._current_pos = await self.get_size()
            self._current_reader = None
            return


        if whence==2:
            offset += await self.get_size()
        elif whence==1:
          offset += self._current_pos
        else:
            pass

        # optimization that stays on the same position if already there, therefore does not open another connection
        if offset == self._current_pos and self._current_reader is not None:
            print("Not seeking, already at the offset")
            return

        # optimization that read some bytes instead seeking, therefore does not open another connection
        if (offset - self._current_pos > 0) and (offset - self._current_pos < 1000): # pragma: no cover
            print(f"Reading instead of seeking, difference is: {offset - self._current_pos}")
            await self.read(offset-self._current_pos)
            return

        if self._response: # pragma: no cover
            await self._response.__aexit__(None, None, None)

        self._response = await self._session.get(self._url, headers={
            'range': f'bytes={offset}-',
            'Accept-Encoding': "identity" # ensure file is not zipped
        })

        if self._response.status != 206: # pragma: no cover
            raise ValueError(f"Server does not support seek(), {offset=}, {self._response.headers['Content-Range']=}")

        self._current_reader = self._response.content
        self._metadata['media_type'] = self._response.headers['Content-Type']
        self._current_pos = offset

    @property
    def metadata(self) -> dict:
        """
        Property holding metadata like file_name, media_type, etc.

        :return: The metadata dictionary.
        """
        return self._metadata # pragma: no cover

    @metadata.setter
    def metadata(self, value: dict) -> None:
        """
        Setter for updating metadata.

        :param value: A dictionary with new metadata values.
        """
        self._metadata.update(value) # pragma: no cover

    async def tell(self) -> int:
        """
        Return the current position in the stream.

        :return: The current position (in bytes) within the stream.
        """
        return self._current_pos

    async def get_size(self) -> int:
        """
        Get the total size of the file being read from the URL.

        This method makes a request to the server to retrieve the content range
        and size of the file. It caches the size after the first request for efficiency.

        :return: The total size of the file in bytes.
        """
        if self._size is not None:
            return self._size

        async with self._session.get(self._url, headers={'range':f'bytes=0-0'}) as response:
            if response.status != 206: # pragma: no cover
                raise ValueError(f"Failed to fetch file from URL: {response.status}")
            self._size = int(response.headers.get("Content-Range").split("/")[-1])
            return self._size
