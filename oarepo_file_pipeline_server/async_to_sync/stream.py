import asyncio


class AsyncToSyncStream:
    def __init__(self, async_stream, even_loop):
        self.async_stream = async_stream
        self.event_loop = even_loop

    def seek(self, offset, pos: int=0):
        #print(f'seeking {offset} to {pos}')
        value = self.async_to_sync(self.async_stream.seek(offset, pos))
        #print(f'seek return {value}')
        return value

    def async_to_sync(self, awaitable):
        return asyncio.run_coroutine_threadsafe(awaitable, self.event_loop).result()


    def tell(self):
       #print(f'telling ')
        value = self.async_to_sync(self.async_stream.tell())
        #print(f'tell return {value}')
        return value

    def read(self, size:int=-1):
        #print(f'reading {size}')
        value = self.async_to_sync(self.async_stream.read(size))
        #print(f'reading return {len(value)}')
        return value

    def readline(self):
        #print(f'readline ')
        value = self.async_to_sync(self.async_stream.readline())
        return value

    @property
    def seekable(self):
        """Property used in zip files"""
        return True


    def seekable(self):
        return True