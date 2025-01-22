"""
Class that represents pipeline data with data read from URL stream
"""
from __future__ import annotations

import io
from typing import TYPE_CHECKING, Self

if TYPE_CHECKING:
    import aiohttp

from oarepo_file_pipeline_server.pipeline_data.pipeline_data import PipelineData


class UrlPipelineData(PipelineData):
    def __init__(self, url: str, session: aiohttp.ClientSession) -> None:
        """Constructor for UrlPipelineData"""
        self._url = url
        self._current_reader = None
        self._response = None
        self._current_pos = 0
        self._size = None
        self._session = session
        self._metadata = dict()

    def __aiter__(self) -> Self :
        return self

    async def __anext__(self) -> bytes:
        """Asynchronously iterate through 65000 Kb of data"""
        chunk =  await self.read(65000)

        if not chunk:
            raise StopAsyncIteration
        return chunk


    def get_stream(self) -> aiohttp.StreamReader:
        """
        Get the input stream.
        :return: The stream object.
        """

        return self._current_reader

    async def read(self, size=-1) -> bytes:
        """"Read size amount of bytes from stream"""
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

    async def seek(self, offset, whence=0):
        """Seek with offset and whence in strem."""
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

        print(f'{self._current_pos=}, want to seek: {offset=}, {whence=}')
        if offset == self._current_pos and self._current_reader is not None:
            print("Not seeking, already at the offset")
            return

        if (offset - self._current_pos > 0) and (offset - self._current_pos < 1000):
            print(f"Reading instead of seeking, difference is: {offset - self._current_pos}")
            await self.read(offset-self._current_pos)
            return

        if self._response:
            await self._response.__aexit__(None, None, None)

        # TODO Check status code etc
        self._response = await self._session.get(self._url, headers={
            'range': f'bytes={offset}-',
            'Accept-Encoding': "identity"
        })

        if self._response.status != 206:
            raise ValueError(f"Server does not support seek(), {offset=}, {self._response.headers['Content-Range']=}")

        self._current_reader = self._response.content
        self._metadata['media_type'] = self._response.headers['Content-Type']
        self._current_pos = offset

    @property
    def metadata(self) -> dict:
        """Property holding metadata like file_name, media_type, etc."""
        return self._metadata

    @metadata.setter
    def metadata(self, value: dict) -> None:
        """Setter for metadata."""
        self._metadata.update(value)

    async def tell(self) -> int:
        """Tell current position in the stream"""
        return self._current_pos

    async def get_size(self) -> int:
        """Get file size"""
        if self._size is not None:
            return self._size

        async with self._session.get(self._url, headers={'range':f'bytes=0-0'}) as response:
            if response.status != 206:
                raise ValueError(f"Failed to fetch file from URL: {response.status}")
            self._size = int(response.headers.get("Content-Range").split("/")[-1])
            return self._size
