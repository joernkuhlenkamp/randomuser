import json
import os
import boto3

s3 = boto3.client('s3')

class LoadError(RuntimeError):
    def __init__(self, message):
        super().__init__(message)

def _get_bucket_name_from_environment():
    bucket_name = os.environ.get('BUCKET_NAME')
    if not bucket_name: raise LoadError('Unable to load BUCKET_NAME from env.')
    return bucket_name

def _load_to_s3(bucket_name: str, records: list = []):
    s3.put_object(
        Body=json.dumps(records),
        Bucket=bucket_name,
        Key='users.json'
    )

def _load_to_s3_with_except(bucket_name: str, records: list = []):
    try:
        _load_to_s3(bucket_name, records)
    except:
        raise LoadError('Unable to load countries to S3 bucket.')

def load(countries: list = []):
    bucket_name = _get_bucket_name_from_environment()
    _load_to_s3_with_except(bucket_name, countries)