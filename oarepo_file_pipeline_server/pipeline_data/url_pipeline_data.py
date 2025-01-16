from oarepo_file_pipeline_server.pipeline_data.pipeline_data import PipelineData


class UrlPipelineData(PipelineData):
    def __init__(self, url, session):
        self._url = url
        self._current_reader = None
        self._current_pos = 0
        self._size = None
        self._session = session
        self._metadata = dict()

    def __aiter__(self):
        return self

    async def __anext__(self):
        chunk =  await self.read()

        if chunk is None:
            raise StopAsyncIteration
        return chunk


    def get_stream(self):
        """
        Get the input stream.
        :return: The stream object.
        """

        return self._current_reader

    async def read(self, size=-1):
        if not self._current_reader:
            await self.seek(0)

        chunk = await self._current_reader.read(size)
        if not chunk:
            return None

        self._current_pos += len(chunk)
        return chunk

    async def seek(self, offset, whence=0):
        if whence==2:
            offset += await self.get_size()
        elif whence==1:
          offset += self._current_pos
        else:
            pass

        # TODO Check status code etc
        response = await self._session.get(self._url, headers={'range': f'bytes={offset}-'})
        #print(f'seek async url headers {response.headers}')
        self._current_reader = response.content
        self._metadata['media_type'] = response.headers['Content-Type']
        self._current_pos = offset
        #print(f'seek async url stream pos {self._current_pos}')

    @property
    def metadata(self) -> dict:
        """Property holding metadata like file_name, media_type, etc."""
        return self._metadata

    @metadata.setter
    def metadata(self, value: dict) -> None:
        """Setter for metadata."""
        self._metadata.update(value)

    async def tell(self):
        return self._current_pos

    async def get_size(self):
        if self._size is not None:
            return self._size

        async with self._session.get(self._url, headers={'range':f'bytes=0-0'}) as response:
            if response.status != 206:
                raise ValueError(f"Failed to fetch file from URL: {response.status}")
            self._size = int(response.headers.get("Content-Range").split("/")[-1])
            return self._size
