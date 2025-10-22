import os
from typing import List

backend_name = os.environ.get("OSSDATA_BACKEND", "OSS")
if backend_name == "OSS":
    from .backend import oss as B
    from .backend.oss import OSS_BUCKET, OSS_DATASET_PATH
elif backend_name == "NAS":
    from .backend import nas as B
    OSS_BUCKET = ""
    OSS_DATASET_PATH = B.NAS_DATASET_PATH
else:
    raise ValueError(f"Unknown backend: {backend_name}")

def get_item(name: str, version: str, instance_id: str, key: str | None = None):
    return B.get_item(name, version, instance_id, key)


def list_dir(path: str) -> List[str]:
    return B.list_dir(path)


def list_objects(path: str) -> List[str]:
    return B.list_objects(path)

def upload(item, name, split, revision, docker_image_prefix):
    return B.upload(item, name, split, revision, docker_image_prefix)

def upload_to_oss(item, name, split, revision, docker_image_prefix):
    return B.upload(item, name, split, revision, docker_image_prefix)

def get_all_datasets() -> List[str]:
    return B.get_all_datasets()


def get_all_versions(name) -> List[str]:
    return B.get_all_versions(name)


def get_all_instance_ids(name, version) -> List[str]:
    return B.get_all_instance_ids(name, version)

