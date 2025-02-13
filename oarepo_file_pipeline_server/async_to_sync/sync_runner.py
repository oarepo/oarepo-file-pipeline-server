#
# Copyright (C) 2025 CESNET z.s.p.o.
#
# oarepo-file-pipeline-server is free software; you can redistribute it and/or
# modify it under the terms of the MIT License; see LICENSE file for more
# details.
#
"""Logic of converting sync stream to async stream."""

import asyncio
import ctypes
import sys
import threading
from oarepo_file_pipeline_server.async_to_sync.stream import AsyncToSyncStream

class ResultQueue:
    """
    A helper class that interfaces with an asyncio.Queue to allow placing results from a synchronous
    function into the queue from a separate thread.
    """
    def __init__(self, queue: asyncio.Queue, loop):
        """Initializes the ResultQueue instance."""
        self.queue = queue
        self.loop = loop

    def put(self, result_type, result_value):
        """Places a result into the queue. Runs the operation asynchronously in the event loop."""
        try:
            asyncio.run_coroutine_threadsafe(self.queue.put((result_type, result_value)),
                                         self.loop).result()
        except:
            print('putting result', result_type, file=sys.stderr, flush=True)
            raise

class StoppableQueue:
    def __init__(self, queue: asyncio.Queue, thread_id):
        self.queue = queue
        self.thread_id = thread_id

    async def get(self):
        return await self.queue.get()

    def stop(self):
        if hasattr(self.queue, 'shutdown'):
            self.queue.shutdown(immediate=True)
        print("Stopping thread", file=sys.stderr, flush=True)
        thread_id = self.thread_id
        res = ctypes.pythonapi.PyThreadState_SetAsyncExc(thread_id,
                                                         ctypes.py_object(SystemExit))
        if res > 1:
            ctypes.pythonapi.PyThreadState_SetAsyncExc(thread_id, 0)
            print('Exception raise failure', file=sys.stderr, flush=True)

async def sync_runner(sync_function, *args,queue_size=1, **kwargs) -> StoppableQueue:
    """
    Runs a synchronous function in a separate thread and returns an asyncio.Queue
    that can be used to fetch the result asynchronously.

    The synchronous function is executed in a separate thread, and its result is placed
    into the queue by using `ResultQueue`. The function is expected to handle its own
    processing and then put the result into the queue.
    """
    q = asyncio.Queue(maxsize=queue_size)
    loop = asyncio.get_running_loop()

    result_queue = ResultQueue(q, loop) # initialize the helper class

    def helper_fn():
        try:
            # Call the sync function
            ret = sync_function(*args, **kwargs, result_queue=result_queue)
            # Put the `complete` tag after finishing
            result_queue.put('complete',ret)
        except Exception as e:
            # In case of error, place the error
            result_queue.put('error', e)

    # Start the helper function in a separate thread
    print(f'before creating thread, {sync_function}', file=sys.stderr, flush=True)
    thread = threading.Thread(target=helper_fn)
    thread.start()
    return StoppableQueue(q, thread.native_id)

async def read_result(queue: asyncio.Queue):
    """Helper function that reads the result from the queue, raises exception if necessary or return the complete result."""
    item_type, item_value = await queue.get()

    if item_type == 'error':
        raise item_value
    elif item_type == 'complete':
        return item_value
    else:
        raise ValueError(f'Unknown item type: {item_type}')

async def sync_stream_runner(sync_function, stream, *args, **kwargs) -> StoppableQueue:
    """
    A helper function to run a synchronous function that takes an async stream,
    converting the async stream to a synchronous stream using `AsyncToSyncStream`.

    This allows synchronous code to interact with asynchronous streams seamlessly.
    """
    return await sync_runner(sync_function, AsyncToSyncStream(stream, asyncio.get_running_loop()), *args, **kwargs)