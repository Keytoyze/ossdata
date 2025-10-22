import os
import json
from datetime import datetime, date
import os
from pathlib import Path
from typing import List

NAS_DATASET_PATH = os.getenv("NAS_DATASET_PATH", "/mnt/Group-code/datasets")

def get_item(name: str, version: str, instance_id: str, key: str | None = None):
    path = Path(NAS_DATASET_PATH) / name / version / f"{instance_id}.json"
    result = path.read_text()
    if key is not None:
        return json.loads(result)[key]
    else:
        return result


def list_dir(path: str) -> List[str]:
    if not os.path.exist(path) or os.path.isfile(path):
        return []
    return [x.rstrip("/") for x in os.listdir(path)]


def list_objects(path: str) -> List[str]:
    return list_dir(path)

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
    root_path = f"{NAS_DATASET_PATH}/{name}/{version}"
    os.makedirs(root_path, exist_ok=True)
    Path(f"{root_path}/{instance_id}.json").write_text(json.dumps(item, default=datetime_serializer))


def get_all_datasets() -> List[str]:
    result = []
    for ds_repo in list_dir(f"{NAS_DATASET_PATH}"):
        for ds_name in list_dir(f"{NAS_DATASET_PATH}/{ds_repo}"):
            result.append(f"{ds_repo}/{ds_name}")
    return result


def get_all_versions(name) -> List[str]:
    return list_dir(f"{NAS_DATASET_PATH}/{name}/")


def get_all_instance_ids(name, version) -> List[str]:
    return [x.replace(".json", "") for x in list_objects(f"{NAS_DATASET_PATH}/{name}/{version}")]

