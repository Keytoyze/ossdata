import os
import json
from datetime import datetime, date
from typing import List
import time
from functools import wraps
import alibabacloud_oss_v2 as oss
import traceback

OSS_BUCKET = os.getenv("OSS_BUCKET", "ofasys-wlcb-toshanghai")
OSS_DATASET_PATH = os.getenv("OSS_DATASET_PATH", "swe/datasets")


def retry(max_retries=100, delay_seconds=1):
    """
    一个装饰器，用于在函数执行失败时自动重试。

    :param max_retries: 最大重试次数。
    :param delay_seconds: 每次重试之间的延迟时间（秒）。
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            attempts = 0
            while attempts < max_retries:
                try:
                    # 尝试执行被装饰的函数
                    return func(*args, **kwargs)
                except Exception as e:
                    attempts += 1
                    if attempts >= max_retries:
                        raise e
                    else:
                        print(f"'{func.__name__}' Error: ({attempts}/{max_retries}): {traceback.format_exc()}, will retry...")
                        time.sleep(delay_seconds)
        return wrapper
    return decorator


def get_client():
    assert "OSS_ACCESS_KEY_ID" in os.environ, "Please set OSS_ACCESS_KEY_ID in environment variables"
    assert "OSS_ACCESS_KEY_SECRET" in os.environ, "Please set OSS_ACCESS_KEY_SECRET in environment variables"
    assert "OSS_REGION" in os.environ, "Please set OSS_REGION in environment variables"
    assert "OSS_ENDPOINT" in os.environ, "Please set OSS_ENDPOINT in environment variables"

    credentials_provider = oss.credentials.EnvironmentVariableCredentialsProvider()
    cfg = oss.config.load_default()
    cfg.retryer = oss.retry.StandardRetryer(max_attempts=1)
    cfg.credentials_provider = credentials_provider
    cfg.region = os.environ["OSS_REGION"]
    cfg.endpoint = os.environ["OSS_ENDPOINT"]
    client = oss.Client(cfg)
    return client


@retry(max_retries=100, delay_seconds=1)
def get_item(name: str, version: str, instance_id: str, key: str | None = None):
    response = get_client().get_object(oss.GetObjectRequest(
        bucket=OSS_BUCKET,  # 指定存储空间名称
        key=f"{OSS_DATASET_PATH}/{name}/{version}/{instance_id}.json",  # 指定对象键名
    ))
    with response.body as body_stream:
        result = body_stream.read().decode()
    if key is not None:
        return json.loads(result)[key]
    else:
        return result


@retry(max_retries=100, delay_seconds=1)
def list_dir(path: str) -> List[str]:
    if not path.endswith("/"):
        path += "/"
    paginator = get_client().list_objects_v2_paginator()
    result = []
    for page in paginator.iter_page(oss.ListObjectsV2Request(
            bucket=OSS_BUCKET,
            prefix=path,
            delimiter="/",
        )
    ):
        if page is not None and page.common_prefixes:
            for prefix in page.common_prefixes:
                result.append(prefix.prefix.replace(path, "").rstrip("/"))
    result = [x for x in result if x.strip() != ""]
    return result


@retry(max_retries=100, delay_seconds=1)
def list_objects(path: str) -> List[str]:
    if not path.endswith("/"):
        path += "/"
    paginator = get_client().list_objects_v2_paginator()
    result = []
    for page in paginator.iter_page(oss.ListObjectsV2Request(
            bucket=OSS_BUCKET,
            prefix=path,
        )
    ):
        if page is not None and page.contents:
            for o in page.contents:
                result.append(o.key.replace(path, ""))
    result = [x for x in result if x.strip() != ""]
    return result

def datetime_serializer(obj):
    """JSON serializer for objects not serializable by default json code"""
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")


@retry(max_retries=100, delay_seconds=1)
def upload(item, name, split, revision, docker_image_prefix):
    instance_id = item['instance_id']
    version = split
    if revision:
        version += f"@{revision}"

    if docker_image_prefix:
        item["docker_image"] = docker_image_prefix + instance_id.lower()
    item["dataset"] = name
    item["split"] = split
    item["revision"] = revision
    key = f"{OSS_DATASET_PATH}/{name}/{version}/{instance_id}.json"

    get_client().put_object(oss.PutObjectRequest(
        bucket=OSS_BUCKET,
        key=key,
        body=json.dumps(item, default=datetime_serializer).encode('utf-8'),
    ))


def get_all_datasets() -> List[str]:
    result = []
    for ds_repo in list_dir(f"{OSS_DATASET_PATH}"):
        for ds_name in list_dir(f"{OSS_DATASET_PATH}/{ds_repo}"):
            result.append(f"{ds_repo}/{ds_name}")
    return result


def get_all_versions(name) -> List[str]:
    return list_dir(f"{OSS_DATASET_PATH}/{name}/")


def get_all_instance_ids(name, version) -> List[str]:
    return [x.replace(".json", "") for x in list_objects(f"{OSS_DATASET_PATH}/{name}/{version}")]

