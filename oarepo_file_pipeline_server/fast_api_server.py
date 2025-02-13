#
# Copyright (C) 2025 CESNET z.s.p.o.
#
# oarepo-file-pipeline-server is free software; you can redistribute it and/or
# modify it under the terms of the MIT License; see LICENSE file for more
# details.
#
"""Fast api endpoints definition"""
from typing import AsyncIterator

from fastapi import FastAPI, HTTPException, Response
from starlette.responses import StreamingResponse

from oarepo_file_pipeline_server.pipeline_data.pipeline_data import PipelineData
from oarepo_file_pipeline_server.pipeline_steps.base import StepResults
from utils import ping_s3_storage, get_payload, get_pipeline_step_obj
from proxies import redis_client
from oarepo_file_pipeline_server.pipeline_steps.create_zip import CreateZip
fast_app = FastAPI()


@fast_app.get("/healthcheck")
async def healthcheck():
    return {
        "redis": redis_client.ping(),
        "s3": ping_s3_storage()
    }


@fast_app.get("/pipeline/{token_id}")
async def process_pipeline(token_id):
    try:
        # Retrieve and delete JWE token from Redis
        jwe_token = redis_client.get(f'{token_id}')
        redis_client.delete(f'{token_id}')

        if jwe_token is None:
            raise HTTPException(status_code=404, detail="Content not found in cache.")

        payload = get_payload(jwe_token) # get payload with pipeline steps

        inputs: AsyncIterator[PipelineData] | None = None
        outputs: StepResults | None = None

        assert len(payload['pipeline_steps']) > 0

        for pipeline_step in payload['pipeline_steps']:
            pipeline_step_obj = get_pipeline_step_obj(pipeline_step['type'])() # initialize step
            print(f"Step {pipeline_step['type']}")
            outputs = await pipeline_step_obj.process(inputs, args=pipeline_step.get('arguments', [])) # initialize step with inputs and args
            inputs = outputs.results # outputs from last step are now inputs to next step

        assert outputs, "Outputs can not be empty"

        if outputs.file_count != 1: # multiple inputs in the last step, create zip
            create_zip = CreateZip()
            result: StepResults = await create_zip.process(inputs, {})
            zip_pipeline_data = await anext(result.results)
            return StreamingResponse(zip_pipeline_data, media_type=zip_pipeline_data.metadata['media_type'],
                                     headers=zip_pipeline_data.metadata.get('headers', {}))

        output = await anext(outputs.results)
        return StreamingResponse(output, media_type=output.metadata['media_type'],
                                 headers=output.metadata.get('headers', {}))


    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error executing pipeline: {str(e)}")
