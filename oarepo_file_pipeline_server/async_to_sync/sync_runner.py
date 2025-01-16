import asyncio
import threading
from oarepo_file_pipeline_server.async_to_sync.stream import AsyncToSyncStream

class ResultQueue:
    def __init__(self, queue: asyncio.Queue, loop):
        self.queue = queue
        self.loop = loop

    def put(self, result_type, result_value):
        asyncio.run_coroutine_threadsafe(self.queue.put((result_type, result_value)),
                                         self.loop).result()

async def sync_runner(sync_function, *args,queue_size=1, **kwargs) -> asyncio.Queue:
    q = asyncio.Queue(maxsize=queue_size)
    loop = asyncio.get_running_loop()

    result_queue = ResultQueue(q, loop)

    def helper_fn():
        try:
            ret = sync_function(*args, **kwargs, result_queue=result_queue)
            result_queue.put('complete',ret)
        except Exception as e:
            result_queue.put('error', e)

    threading.Thread(target=helper_fn).start()
    return q

async def read_result(queue: asyncio.Queue):
    item_type, item_value = await queue.get()

    if item_type == 'error':
        raise item_value
    elif item_type == 'complete':
        return item_value
    else:
        raise ValueError(f'Unknown item type: {item_type}')

async def sync_stream_runner(sync_function, stream, *args, **kwargs) -> asyncio.Queue:
    return await sync_runner(sync_function, AsyncToSyncStream(stream, asyncio.get_running_loop()), *args, **kwargs)