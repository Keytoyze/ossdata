import os
import json
from datetime import datetime, date
import os
from pathlib import Path
from typing import List

NAS_DATASET_PATH = os.getenv("NAS_DATASET_PATH", "/mnt/Group-code/datasets")

def get_item(name: str, version: str, instance_id: str, key: str | None = None, **kwargs):
    """
    Retrieve an item from NAS storage.
    
    :param name: Dataset name
    :param version: Dataset version
    :param instance_id: Instance identifier
    :param key: Specific key to extract from the JSON object
    :return: Item content or specific value from the item
    """
    path = Path(NAS_DATASET_PATH) / name / version / f"{instance_id}.json"
    result = path.read_text()
    if key is not None:
        return json.loads(result)[key]
    else:
        return result


def list_dir(path: str, **kwargs) -> List[str]:
    """
    List directories under a given path in NAS storage.
    
    :param path: Path to list directories from
    :return: List of directory names
    """
    if not os.path.exists(path) or os.path.isfile(path):
        return []
    return [x.rstrip("/") for x in os.listdir(path)]


def list_objects(path: str, **kwargs) -> List[str]:
    """
    List objects under a given path in NAS storage.
    
    :param path: Path to list objects from
    :return: List of object names
    """
    return list_dir(path, **kwargs)

def datetime_serializer(obj):
    """
    JSON serializer for objects not serializable by default json code.
    
    :param obj: Object to serialize
    :return: ISO formatted string for datetime/date objects
    """
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")


def upload(item, name, split, revision, docker_image_prefix, **kwargs):
    """
    Upload an item to NAS storage.
    
    :param item: Data item to upload
    :param name: Dataset name
    :param split: Dataset split
    :param revision: Dataset revision
    :param docker_image_prefix: Docker image prefix
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
    root_path = f"{NAS_DATASET_PATH}/{name}/{version}"
    os.makedirs(root_path, exist_ok=True)
    Path(f"{root_path}/{instance_id}.json").write_text(json.dumps(item, default=datetime_serializer))


def get_all_datasets(**kwargs) -> List[str]:
    """
    Get all available datasets.
    
    :return: List of dataset names
    """
    result = []
    for ds_repo in list_dir(f"{NAS_DATASET_PATH}", **kwargs):
        for ds_name in list_dir(f"{NAS_DATASET_PATH}/{ds_repo}", **kwargs):
            result.append(f"{ds_repo}/{ds_name}")
    return result


def get_all_versions(name, **kwargs) -> List[str]:
    """
    Get all versions of a specific dataset.
    
    :param name: Dataset name
    :return: List of dataset versions
    """
    return list_dir(f"{NAS_DATASET_PATH}/{name}/", **kwargs)


def get_all_instance_ids(name, version, **kwargs) -> List[str]:
    """
    Get all instance IDs of a specific dataset version.
    
    :param name: Dataset name
    :param version: Dataset version
    :return: List of instance IDs
    """
    return [x.replace(".json", "") for x in list_objects(f"{NAS_DATASET_PATH}/{name}/{version}", **kwargs)]
