from fastapi import FastAPI, HTTPException, Response
from starlette.responses import StreamingResponse
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

        payload = get_payload(jwe_token)

        inputs = outputs = None
        multiple_outputs = False
        for pipeline_step in payload['pipeline_steps']:
            pipeline_step_obj = get_pipeline_step_obj(pipeline_step['type'])()
            print(f"Step {pipeline_step['type']}")
            outputs = pipeline_step_obj.process(inputs, args=pipeline_step.get('arguments', []))
            multiple_outputs = multiple_outputs or pipeline_step_obj.produces_multiple_outputs
            inputs = outputs

        if multiple_outputs:
            create_zip = CreateZip()
            result = create_zip.process(inputs, {})
            zip_pipeline_data = await anext(result)
            return StreamingResponse(zip_pipeline_data, media_type=zip_pipeline_data.metadata['media_type'],
                                     headers=zip_pipeline_data.metadata.get('headers', {}))

        output = await anext(outputs)
        return StreamingResponse(output, media_type=output.metadata['media_type'],
                                 headers=output.metadata.get('headers', {}))


    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error executing pipeline: {str(e)}")
