#
# Copyright (C) 2025 CESNET z.s.p.o.
#
# oarepo-file-pipeline-server is free software; you can redistribute it and/or
# modify it under the terms of the MIT License; see LICENSE file for more
# details.
#
# TODO
from typing import AsyncIterator

import crypt4gh

from oarepo_file_pipeline_server.async_to_sync.sync_runner import sync_stream_runner, ResultQueue
from oarepo_file_pipeline_server.pipeline_data.async_writer import AsyncWriter
from oarepo_file_pipeline_server.pipeline_data.pipeline_data import PipelineData
from oarepo_file_pipeline_server.pipeline_data.queue_pipeline_data import QueuePipelineData
from oarepo_file_pipeline_server.pipeline_data.url_pipeline_data import UrlPipelineData
from oarepo_file_pipeline_server.pipeline_steps.base import PipelineStep


class Crypt4GH(PipelineStep):
    async def process(self, inputs: AsyncIterator[PipelineData] | None, args: dict) -> AsyncIterator[PipelineData] | None:
        if not inputs and not args:
            raise ValueError("No input data or arguments were provided to Crypt4GH step.")
        if inputs:
            assert not isinstance(inputs, PipelineData)

            input_stream = await anext(inputs)
        elif args and "source_url" in args:
            from oarepo_file_pipeline_server.utils import http_session
            input_stream = UrlPipelineData(args["source_url"], http_session)
        else:
            raise ValueError("No input nor source_url were provided.")

        recipient_pub = args.get("recipient_pub")
        results = await sync_stream_runner(crypt4gh_add_recipient, input_stream, recipient_pub)

        item_type, item_value = await results.get()
        while item_type != 'complete':
            if item_type == 'error':
                raise item_value

            if item_type != 'startfile':
                raise ValueError(f"Implementation error: {item_type}")

            yield QueuePipelineData(results, metadata=item_value)
            item_type, item_value = await results.get()


def crypt4gh_add_recipient(input_stream, recipient_pub: str, result_queue: ResultQueue):
    from oarepo_c4gh import Crypt4GHWriter, AddRecipientFilter, C4GHKey, Crypt4GH
    from oarepo_c4gh.key.key_collection import KeyCollection
    from oarepo_file_pipeline_server.config import server_key_priv_c4gh

    if not recipient_pub:
        raise ValueError("No recipient public key was provided.")

    server_key = C4GHKey.from_string(server_key_priv_c4gh)
    recipient_pub = C4GHKey.from_string(recipient_pub)

    crypt4gh = Crypt4GH(server_key, input_stream)
    filter4gh = AddRecipientFilter(crypt4gh, recipient_pub.public_key)

    with AsyncWriter({}, result_queue) as async_writer:
        writer = Crypt4GHWriter(filter4gh, async_writer)
        writer.write()










