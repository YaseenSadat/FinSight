"""
MinIO / S3 helper utilities.

Wraps the Airflow minio_s3 connection to provide a pre-configured boto3 client,
s3fs-compatible storage options for pandas/Spark readers, and a convenience
function for writing JSON objects to a bucket.
"""
from __future__ import annotations
import json
from typing import Tuple

import boto3
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook # type: ignore

from . import config


def get_client() -> Tuple[boto3.client, dict, object]:
    """
    Return a boto3 S3 client plus the raw connection extras and credentials.

    Reads the minio_s3 Airflow connection to obtain the endpoint URL,
    access key, and secret key.

    Returns:
        (boto3 S3 client, extra_dejson dict, AwsCredentials namedtuple)
    """
    hook = AwsBaseHook(aws_conn_id=config.AWS_CONN_ID, client_type="s3")
    creds = hook.get_credentials()
    extra = hook.get_connection(hook.aws_conn_id).extra_dejson
    endpoint_url = extra.get("endpoint_url")

    client = boto3.client(
        "s3",
        aws_access_key_id=creds.access_key,
        aws_secret_access_key=creds.secret_key,
        aws_session_token=creds.token,
        endpoint_url=endpoint_url,
        region_name=extra.get("region_name", "us-east-1"),
    )
    return client, extra, creds


def storage_options() -> dict:
    """
    Return s3fs-compatible storage options for use with pandas and Spark readers.

    Example usage:
        df = pd.read_parquet("s3://bucket/path/", storage_options=storage_options())
    """
    _, extra, creds = get_client()
    return {
        "key": creds.access_key,
        "secret": creds.secret_key,
        "client_kwargs": {"endpoint_url": extra.get("endpoint_url")},
    }


def put_json(client, bucket: str, key: str, payload: dict) -> None:
    """
    Serialise a dict to JSON and upload it to MinIO.

    Args:
        client:   boto3 S3 client.
        bucket:   Target bucket name.
        key:      Object key (path within the bucket).
        payload:  Dict to serialise and upload.
    """
    client.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(payload).encode("utf-8"),
        ContentType="application/json",
    )
