import redis
import boto3

redis_client = redis.Redis(host='localhost', port=6579, db=0)
s3_client = boto3.client('s3',
                         aws_access_key_id="aa-nr_docs-aa",
                         aws_secret_access_key="aaa-nr_docs-aaa",
                         endpoint_url="http://localhost:9000",
                         use_ssl=False
)