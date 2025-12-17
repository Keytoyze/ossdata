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

def get_item(name: str, version: str, instance_id: str, key: str | None = None, 
             oss_access_key_id=None, oss_access_key_secret=None, oss_region=None, oss_endpoint=None):
    """
    Retrieve an item from storage.
    
    :param name: Dataset name
    :param version: Dataset version (=split@revision)
    :param instance_id: Instance identifier
    :param key: Specific key to extract from the JSON object
    :param oss_access_key_id: OSS access key ID, if None uses environment variable
    :param oss_access_key_secret: OSS access key secret, if None uses environment variable
    :param oss_region: OSS region, if None uses environment variable
    :param oss_endpoint: OSS endpoint, if None uses environment variable
    :return: Item content or specific value from the item
    """
    return B.get_item(name, version, instance_id, key,
                     oss_access_key_id=oss_access_key_id,
                     oss_access_key_secret=oss_access_key_secret,
                     oss_region=oss_region,
                     oss_endpoint=oss_endpoint)


def list_dir(path: str, oss_access_key_id=None, oss_access_key_secret=None, oss_region=None, oss_endpoint=None) -> List[str]:
    """
    List directories under a given path in storage.
    
    :param path: Path to list directories from
    :param oss_access_key_id: OSS access key ID, if None uses environment variable
    :param oss_access_key_secret: OSS access key secret, if None uses environment variable
    :param oss_region: OSS region, if None uses environment variable
    :param oss_endpoint: OSS endpoint, if None uses environment variable
    :return: List of directory names
    """
    return B.list_dir(path,
                     oss_access_key_id=oss_access_key_id,
                     oss_access_key_secret=oss_access_key_secret,
                     oss_region=oss_region,
                     oss_endpoint=oss_endpoint)


def list_objects(path: str, oss_access_key_id=None, oss_access_key_secret=None, oss_region=None, oss_endpoint=None) -> List[str]:
    """
    List objects under a given path in storage.
    
    :param path: Path to list objects from
    :param oss_access_key_id: OSS access key ID, if None uses environment variable
    :param oss_access_key_secret: OSS access key secret, if None uses environment variable
    :param oss_region: OSS region, if None uses environment variable
    :param oss_endpoint: OSS endpoint, if None uses environment variable
    :return: List of object names
    """
    return B.list_objects(path,
                         oss_access_key_id=oss_access_key_id,
                         oss_access_key_secret=oss_access_key_secret,
                         oss_region=oss_region,
                         oss_endpoint=oss_endpoint)

def upload(item, name, split, revision, docker_image_prefix, 
           oss_access_key_id=None, oss_access_key_secret=None, oss_region=None, oss_endpoint=None):
    """
    Upload an item to storage.
    
    :param item: Data item to upload
    :param name: Dataset name
    :param split: Dataset split
    :param revision: Dataset revision
    :param docker_image_prefix: Docker image prefix
    :param oss_access_key_id: OSS access key ID, if None uses environment variable
    :param oss_access_key_secret: OSS access key secret, if None uses environment variable
    :param oss_region: OSS region, if None uses environment variable
    :param oss_endpoint: OSS endpoint, if None uses environment variable
    """
    return B.upload(item, name, split, revision, docker_image_prefix,
                   oss_access_key_id=oss_access_key_id,
                   oss_access_key_secret=oss_access_key_secret,
                   oss_region=oss_region,
                   oss_endpoint=oss_endpoint)

def upload_to_oss(item, name, split, revision, docker_image_prefix, 
                  oss_access_key_id=None, oss_access_key_secret=None, oss_region=None, oss_endpoint=None):
    """
    Upload an item to OSS storage.
    
    :param item: Data item to upload
    :param name: Dataset name
    :param split: Dataset split
    :param revision: Dataset revision
    :param docker_image_prefix: Docker image prefix
    :param oss_access_key_id: OSS access key ID, if None uses environment variable
    :param oss_access_key_secret: OSS access key secret, if None uses environment variable
    :param oss_region: OSS region, if None uses environment variable
    :param oss_endpoint: OSS endpoint, if None uses environment variable
    """
    return B.upload(item, name, split, revision, docker_image_prefix,
                   oss_access_key_id=oss_access_key_id,
                   oss_access_key_secret=oss_access_key_secret,
                   oss_region=oss_region,
                   oss_endpoint=oss_endpoint)

def get_all_datasets(oss_access_key_id=None, oss_access_key_secret=None, oss_region=None, oss_endpoint=None) -> List[str]:
    """
    Get all available datasets.
    
    :param oss_access_key_id: OSS access key ID, if None uses environment variable
    :param oss_access_key_secret: OSS access key secret, if None uses environment variable
    :param oss_region: OSS region, if None uses environment variable
    :param oss_endpoint: OSS endpoint, if None uses environment variable
    :return: List of dataset names
    """
    return B.get_all_datasets(oss_access_key_id=oss_access_key_id,
                             oss_access_key_secret=oss_access_key_secret,
                             oss_region=oss_region,
                             oss_endpoint=oss_endpoint)


def get_all_versions(name, oss_access_key_id=None, oss_access_key_secret=None, oss_region=None, oss_endpoint=None) -> List[str]:
    """
    Get all versions of a specific dataset.
    
    :param name: Dataset name
    :param oss_access_key_id: OSS access key ID, if None uses environment variable
    :param oss_access_key_secret: OSS access key secret, if None uses environment variable
    :param oss_region: OSS region, if None uses environment variable
    :param oss_endpoint: OSS endpoint, if None uses environment variable
    :return: List of dataset versions
    """
    return B.get_all_versions(name,
                             oss_access_key_id=oss_access_key_id,
                             oss_access_key_secret=oss_access_key_secret,
                             oss_region=oss_region,
                             oss_endpoint=oss_endpoint)


def get_all_instance_ids(name, version, oss_access_key_id=None, oss_access_key_secret=None, oss_region=None, oss_endpoint=None) -> List[str]:
    """
    Get all instance IDs of a specific dataset version.
    
    :param name: Dataset name
    :param version: Dataset version (=split@revision)
    :param oss_access_key_id: OSS access key ID, if None uses environment variable
    :param oss_access_key_secret: OSS access key secret, if None uses environment variable
    :param oss_region: OSS region, if None uses environment variable
    :param oss_endpoint: OSS endpoint, if None uses environment variable
    :return: List of instance IDs
    """
    return B.get_all_instance_ids(name, version,
                                 oss_access_key_id=oss_access_key_id,
                                 oss_access_key_secret=oss_access_key_secret,
                                 oss_region=oss_region,
                                 oss_endpoint=oss_endpoint)
