
from fastapi import FastAPI, HTTPException, Response
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

        inputs = outputs = []
        for pipeline_step in payload['pipeline_steps']:
            pipeline_step_obj = get_pipeline_step_obj(pipeline_step['type'])()
            print(f"Step {pipeline_step['type']}")
            outputs = await pipeline_step_obj.process(inputs, args=pipeline_step.get('arguments', []))
            inputs = outputs

        if len(outputs) >= 2:
            outputs = await CreateZip().process(inputs=outputs, args={})

        stream = outputs[0].get_stream()
        stream_bytes = stream.read()
        return Response(status_code=200,
                        content=stream_bytes,
                        media_type=outputs[0].metadata['media_type'],
                        headers=outputs[0].metadata.get('headers', {}))

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error executing pipeline: {str(e)}")
