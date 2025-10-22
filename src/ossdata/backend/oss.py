import os
import json
from datetime import datetime, date
import os
from typing import List
import alibabacloud_oss_v2 as oss

OSS_BUCKET = os.getenv("OSS_BUCKET", "ofasys-ap")
OSS_DATASET_PATH = os.getenv("OSS_DATASET_PATH", "datasets")

def get_client():
    assert "OSS_ACCESS_KEY_ID" in os.environ, "Please set OSS_ACCESS_KEY_ID in environment variables"
    assert "OSS_ACCESS_KEY_SECRET" in os.environ, "Please set OSS_ACCESS_KEY_SECRET in environment variables"
    assert "OSS_REGION" in os.environ, "Please set OSS_REGION in environment variables"
    assert "OSS_ENDPOINT" in os.environ, "Please set OSS_ENDPOINT in environment variables"

    credentials_provider = oss.credentials.EnvironmentVariableCredentialsProvider()
    cfg = oss.config.load_default()
    cfg.retryer = oss.retry.StandardRetryer(max_attempts=40)
    cfg.credentials_provider = credentials_provider
    cfg.region = os.environ["OSS_REGION"]
    cfg.endpoint = os.environ["OSS_ENDPOINT"]
    client = oss.Client(cfg)
    return client


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
    return result


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
    return result

def datetime_serializer(obj):
    """JSON serializer for objects not serializable by default json code"""
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")


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

