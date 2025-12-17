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
    A decorator that automatically retries when a function fails to execute.

    :param max_retries: Maximum number of retries.
    :param delay_seconds: Delay time between each retry (seconds).
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            attempts = 0
            while attempts < max_retries:
                try:
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


def get_client(oss_access_key_id=None, oss_access_key_secret=None, oss_region=None, oss_endpoint=None):
    """
    Create and return an OSS client instance.
    
    :param oss_access_key_id: OSS access key ID, if None uses environment variable
    :param oss_access_key_secret: OSS access key secret, if None uses environment variable
    :param oss_region: OSS region, if None uses environment variable
    :param oss_endpoint: OSS endpoint, if None uses environment variable
    :return: OSS client instance
    """
    if oss_access_key_id is not None and oss_access_key_secret is not None:
        credentials_provider = oss.credentials.StaticCredentialsProvider(
            access_key_id=oss_access_key_id,
            access_key_secret=oss_access_key_secret
        )
    else:
        assert "OSS_ACCESS_KEY_ID" in os.environ, "Please set OSS_ACCESS_KEY_ID in environment variables"
        assert "OSS_ACCESS_KEY_SECRET" in os.environ, "Please set OSS_ACCESS_KEY_SECRET in environment variables"
        credentials_provider = oss.credentials.EnvironmentVariableCredentialsProvider()
    
    cfg = oss.config.load_default()
    cfg.retryer = oss.retry.StandardRetryer(max_attempts=1)
    cfg.credentials_provider = credentials_provider
    
    cfg.region = oss_region or os.environ["OSS_REGION"]
    cfg.endpoint = oss_endpoint or os.environ["OSS_ENDPOINT"]
    
    client = oss.Client(cfg)
    return client


@retry(max_retries=100, delay_seconds=1)
def get_item(name: str, version: str, instance_id: str, key: str | None = None, 
             oss_access_key_id=None, oss_access_key_secret=None, oss_region=None, oss_endpoint=None):
    """
    Retrieve an item from OSS storage.
    
    :param name: Dataset name
    :param version: Dataset version
    :param instance_id: Instance identifier
    :param key: Specific key to extract from the JSON object
    :param oss_access_key_id: OSS access key ID
    :param oss_access_key_secret: OSS access key secret
    :param oss_region: OSS region
    :param oss_endpoint: OSS endpoint
    :return: Item content or specific value from the item
    """
    response = get_client(
        oss_access_key_id=oss_access_key_id,
        oss_access_key_secret=oss_access_key_secret,
        oss_region=oss_region,
        oss_endpoint=oss_endpoint
    ).get_object(oss.GetObjectRequest(
        bucket=OSS_BUCKET,
        key=f"{OSS_DATASET_PATH}/{name}/{version}/{instance_id}.json",
    ))
    with response.body as body_stream:
        result = body_stream.read().decode()
    if key is not None:
        return json.loads(result)[key]
    else:
        return result


@retry(max_retries=100, delay_seconds=1)
def list_dir(path: str, oss_access_key_id=None, oss_access_key_secret=None, oss_region=None, oss_endpoint=None) -> List[str]:
    """
    List directories under a given path in OSS storage.
    
    :param path: Path to list directories from
    :param oss_access_key_id: OSS access key ID
    :param oss_access_key_secret: OSS access key secret
    :param oss_region: OSS region
    :param oss_endpoint: OSS endpoint
    :return: List of directory names
    """
    if not path.endswith("/"):
        path += "/"
    paginator = get_client(
        oss_access_key_id=oss_access_key_id,
        oss_access_key_secret=oss_access_key_secret,
        oss_region=oss_region,
        oss_endpoint=oss_endpoint
    ).list_objects_v2_paginator()
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
def list_objects(path: str, oss_access_key_id=None, oss_access_key_secret=None, oss_region=None, oss_endpoint=None) -> List[str]:
    """
    List objects under a given path in OSS storage.
    
    :param path: Path to list objects from
    :param oss_access_key_id: OSS access key ID
    :param oss_access_key_secret: OSS access key secret
    :param oss_region: OSS region
    :param oss_endpoint: OSS endpoint
    :return: List of object names
    """
    if not path.endswith("/"):
        path += "/"
    paginator = get_client(
        oss_access_key_id=oss_access_key_id,
        oss_access_key_secret=oss_access_key_secret,
        oss_region=oss_region,
        oss_endpoint=oss_endpoint
    ).list_objects_v2_paginator()
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
    """
    JSON serializer for objects not serializable by default json code.
    
    :param obj: Object to serialize
    :return: ISO formatted string for datetime/date objects
    """
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")


@retry(max_retries=100, delay_seconds=1)
def upload(item, name, split, revision, docker_image_prefix, 
           oss_access_key_id=None, oss_access_key_secret=None, oss_region=None, oss_endpoint=None):
    """
    Upload an item to OSS storage.
    
    :param item: Data item to upload
    :param name: Dataset name
    :param split: Dataset split
    :param revision: Dataset revision
    :param docker_image_prefix: Docker image prefix
    :param oss_access_key_id: OSS access key ID
    :param oss_access_key_secret: OSS access key secret
    :param oss_region: OSS region
    :param oss_endpoint: OSS endpoint
    """
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

    get_client(
        oss_access_key_id=oss_access_key_id,
        oss_access_key_secret=oss_access_key_secret,
        oss_region=oss_region,
        oss_endpoint=oss_endpoint
    ).put_object(oss.PutObjectRequest(
        bucket=OSS_BUCKET,
        key=key,
        body=json.dumps(item, default=datetime_serializer).encode('utf-8'),
    ))


def get_all_datasets(oss_access_key_id=None, oss_access_key_secret=None, oss_region=None, oss_endpoint=None) -> List[str]:
    """
    Get all available datasets.
    
    :param oss_access_key_id: OSS access key ID
    :param oss_access_key_secret: OSS access key secret
    :param oss_region: OSS region
    :param oss_endpoint: OSS endpoint
    :return: List of dataset names
    """
    result = []
    for ds_repo in list_dir(f"{OSS_DATASET_PATH}", 
                           oss_access_key_id=oss_access_key_id,
                           oss_access_key_secret=oss_access_key_secret,
                           oss_region=oss_region,
                           oss_endpoint=oss_endpoint):
        for ds_name in list_dir(f"{OSS_DATASET_PATH}/{ds_repo}", 
                               oss_access_key_id=oss_access_key_id,
                               oss_access_key_secret=oss_access_key_secret,
                               oss_region=oss_region,
                               oss_endpoint=oss_endpoint):
            result.append(f"{ds_repo}/{ds_name}")
    return result


def get_all_versions(name, oss_access_key_id=None, oss_access_key_secret=None, oss_region=None, oss_endpoint=None) -> List[str]:
    """
    Get all versions of a specific dataset.
    version = split@revision
    
    :param name: Dataset name
    :param oss_access_key_id: OSS access key ID
    :param oss_access_key_secret: OSS access key secret
    :param oss_region: OSS region
    :param oss_endpoint: OSS endpoint
    :return: List of dataset versions
    """
    return list_dir(f"{OSS_DATASET_PATH}/{name}/", 
                   oss_access_key_id=oss_access_key_id,
                   oss_access_key_secret=oss_access_key_secret,
                   oss_region=oss_region,
                   oss_endpoint=oss_endpoint)


def get_all_instance_ids(name, version, oss_access_key_id=None, oss_access_key_secret=None, oss_region=None, oss_endpoint=None) -> List[str]:
    """
    Get all instance IDs of a specific dataset version.
    
    :param name: Dataset name
    :param version: Dataset version
    :param oss_access_key_id: OSS access key ID
    :param oss_access_key_secret: OSS access key secret
    :param oss_region: OSS region
    :param oss_endpoint: OSS endpoint
    :return: List of instance IDs
    """
    return [x[:-5] if x.endswith('.json') else x for x in list_objects(f"{OSS_DATASET_PATH}/{name}/{version}", 
                                                                    oss_access_key_id=oss_access_key_id,
                                                                    oss_access_key_secret=oss_access_key_secret,
                                                                    oss_region=oss_region,
                                                                    oss_endpoint=oss_endpoint)]
