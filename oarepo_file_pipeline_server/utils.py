import importlib

import aiohttp

from oarepo_file_pipeline_server.proxies import s3_client
from joserfc import jwe, jwt
from oarepo_file_pipeline_server.config import server_private_key, repo_public_key, STEP_DEFINITIONS
import time
#from invenio_base.utils import obj_or_import_string
from botocore.exceptions import ClientError
import mimetypes
import os



def get_payload(jwe_token):
    """Decrypts jwe token, then jws token and gets payload """
    jwe_token = jwe_token.decode('utf-8')
    decrypted_jwe = jwe.decrypt_compact(jwe_token, server_private_key)
    encrypted_jwt = decrypted_jwe.plaintext
    claims_requests = jwt.JWTClaimsRegistry(
        now=int(time.time()), leeway=5
    )
    decrypted_jwt = jwt.decode(encrypted_jwt, repo_public_key)
    claims_requests.validate_exp(value=decrypted_jwt.claims.pop('exp'))
    claims_requests.validate_iat(value=decrypted_jwt.claims.pop('iat'))

    payload_data = decrypted_jwt.claims
    return payload_data

def get_pipeline_step_obj(name):
    """Get pipeline step object by name"""
    pipeline_step = STEP_DEFINITIONS.get(name, None)
    if pipeline_step is None:
        raise ValueError(f"PIPELINE_STEP {name} is not defined")

    if isinstance(pipeline_step, str):
        module_name, class_name = pipeline_step.rsplit('.', 1)
        module = importlib.import_module(module_name)
        pipeline_step_obj = getattr(module, class_name)
    else:
        pipeline_step_obj = pipeline_step

    return pipeline_step_obj

def ping_s3_storage():
    """Ping s3 storage for availability"""
    try:
        # Attempt to list buckets (a basic test to see if we can reach the service)
        response = s3_client.list_buckets()
        print("Successfully pinged S3. Buckets:", response['Buckets'])
    except ClientError as e:
        # Handle any errors that occur (e.g., access issues, incorrect keys)
        print(f"Error pinging S3: {e}")
        return False
    return True

http_session = aiohttp.ClientSession()





