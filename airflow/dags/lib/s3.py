"""MinIO/S3 helper utilities."""
from __future__ import annotations
import json
from typing import Tuple

import boto3
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook

from . import config


def get_client() -> Tuple[boto3.client, dict, object]:
    """Return a boto3 S3 client plus connection extras and creds."""
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
    """Return s3fs-compatible storage options for pandas/spark readers."""
    _, extra, creds = get_client()
    return {
        "key": creds.access_key,
        "secret": creds.secret_key,
        "client_kwargs": {"endpoint_url": extra.get("endpoint_url")},
    }


def put_json(client, bucket: str, key: str, payload: dict) -> None:
    client.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(payload).encode("utf-8"),
        ContentType="application/json",
    )
