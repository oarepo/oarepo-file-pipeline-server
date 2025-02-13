#
# Copyright (C) 2025 CESNET z.s.p.o.
#
# oarepo-file-pipeline-server is free software; you can redistribute it and/or
# modify it under the terms of the MIT License; see LICENSE file for more
# details.
#
"""Class that wraps asynchronous stream into synchronous"""

import asyncio
import sys


class AsyncToSyncStream:
    """
    A class that wraps an asynchronous stream and provides a synchronous interface for interacting with it.
    """
    def __init__(self, async_stream, even_loop):
        """Initializes the AsyncToSyncStream instance."""
        self.async_stream = async_stream
        self.event_loop = even_loop

    def seek(self, offset, whence: int=0):
        """Seeks to a specific position in the stream."""
        value = self.async_to_sync(self.async_stream.seek(offset, whence))
        return value

    def async_to_sync(self, awaitable):
        """Converts an asynchronous operation to a synchronous one by running it in the event loop"""
        try:
            return asyncio.run_coroutine_threadsafe(awaitable, self.event_loop).result()
        except:
            print("ASYNC TO SYNC ERROR", awaitable, file=sys.stderr, flush=True)
            raise


    def tell(self):
        """Returns the current position in the stream."""
        value = self.async_to_sync(self.async_stream.tell())
        return value

    def read(self, size:int=-1):
        """Reads up to the specified number of bytes from the stream."""
        value = self.async_to_sync(self.async_stream.read(size))
        return value

    def readline(self):
        """Reads a single line from the stream."""
        value = self.async_to_sync(self.async_stream.readline())
        return value

    @property
    def seekable(self):
        """Property indicating that this stream is seekable. Used in ZIP files."""
        return True


    def seekable(self):
        """
        Returns whether the stream supports seeking.

        This method is just a placeholder for compatibility and always returns True. Used in ZIP files.
        """
        return True