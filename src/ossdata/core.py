import os
import subprocess
from pathlib import Path
import json
from datetime import datetime, date
import os
import time
from typing import List


OSS_DATASET_PATH = os.getenv("OSS_BASE_PATH", "oss://ofasys-ap/datasets")
assert "OSS_ACCESS_KEY_ID" in os.environ, "Please set OSS_ACCESS_KEY_ID in environment variables"
assert "OSS_ACCESS_KEY_SECRET" in os.environ, "Please set OSS_ACCESS_KEY_SECRET in environment variables"
assert "OSS_REGION" in os.environ, "Please set OSS_REGION in environment variables"
assert "OSS_ENDPOINT" in os.environ, "Please set OSS_ENDPOINT in environment variables"


def execute_oss_commands(args, retries=40):
    args = ["ossutil"] + args
    while True:
        try:
            return subprocess.run(args, check=True, capture_output=True, text=True).stdout
        except subprocess.CalledProcessError as e:
            print(e.stderr)
            retries -= 1
            print(f"======= ERROR happens in command: {args}. Remaining retry count: {retries} =======")
            if retries <= 0:
                raise e
            time.sleep(3)


def get_item(name: str, version: str, instance_id: str, key: str | None):
    result = execute_oss_commands([
        "cat", f"{OSS_DATASET_PATH}/{name}/{version}/{instance_id}.json", "-q"
    ])
    if key is not None:
        return json.loads(result)[key]
    else:
        return result


def list_dir(path: str) -> List[str]:
    if not path.endswith("/"):
        path += "/"
    result = [
        x.strip().replace(path, "").rstrip("/") for x in execute_oss_commands(
            ["ls", path, "--short-format", "-qd"]
        ).splitlines() if x.startswith("oss://")
    ]
    return result


def datetime_serializer(obj):
    """JSON serializer for objects not serializable by default json code"""
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")


def upload_to_oss(item, name, split, revision, docker_image_prefix):
    instance_id = item['instance_id']
    version = split
    if revision:
        version += f"@{revision}"

    if docker_image_prefix:
        item["docker_image"] = docker_image_prefix + instance_id.lower()
    item["dataset"] = name
    item["split"] = split
    item["revision"] = revision
    temp_path = Path(f"/dev/shm/{instance_id}.json")
    try:
        temp_path.write_text(json.dumps(item, default=datetime_serializer))
        execute_oss_commands([
            "cp", "-f", 
            str(temp_path.resolve()), f"{OSS_DATASET_PATH}/{name}/{version}/{instance_id}.json", 
            "--retry-times=500", "-u"
        ])
    finally:
        temp_path.unlink(missing_ok=True)


def get_all_datasets() -> List[str]:
    result = []
    for ds_repo in list_dir(OSS_DATASET_PATH):
        for ds_name in list_dir(f"{OSS_DATASET_PATH}/{ds_repo}"):
            result.append(f"{ds_repo}/{ds_name}")
    return result

def get_all_versions(name) -> List[str]:
    return list_dir(f"{OSS_DATASET_PATH}/{name}/")


def get_all_instance_ids(name, version) -> List[str]:
    return [x.replace(".json", "") for x in list_dir(f"{OSS_DATASET_PATH}/{name}/{version}")]


