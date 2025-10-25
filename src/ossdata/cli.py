#!/usr/bin/env python3
import argparse
import sys, os
from tqdm import tqdm
import functools
import multiprocessing
from pathlib import Path
import json
from ossdata.core import upload_to_oss, get_item, get_all_datasets, get_all_versions, get_all_instance_ids, list_objects, OSS_DATASET_PATH


def main():
    parser = argparse.ArgumentParser(
        prog="ossdata",
        description="A CLI tool to manage SWE datasets from HuggingFace to OSS",
    )
    subparsers = parser.add_subparsers(dest="command", title="commands", required=True)

    upload_parser = subparsers.add_parser(
        "upload",
        help="Upload a dataset split from Hugging Face to OSS"
    )
    upload_parser.add_argument("--name", default=None, help="Dataset name (e.g., 'princeton-nlp/SWE-bench')")
    upload_parser.add_argument("--split", default=None, help="Dataset split (e.g., 'test', 'train')")
    upload_parser.add_argument("--revision", help="Optional Hugging Face dataset revision")
    upload_parser.add_argument("--input-file", help="Input json lines file")
    upload_parser.add_argument("--docker-image-prefix", help="Docker image prefix for instances")
    upload_parser.add_argument("-j", default=32, type=int, help="Number of parallel jobs")
    upload_parser.add_argument("--force", action="store_true", help="Force update the dataset if exists")

    download_parser = subparsers.add_parser(
        "download",
        help="Download an OSS dataset as a jsonl file."
    )
    download_parser.add_argument("--name", default=None, help="Dataset name (e.g., 'princeton-nlp/SWE-bench'). If not specified, download all datasets")
    download_parser.add_argument("--version", default=None, help="Dataset version, (e.g., 'test', 'train'). If not specified, download all versions")
    download_parser.add_argument("--output-file", required=True, help="Output json line file")
    download_parser.add_argument("-j", default=32, type=int, help="Number of parallel jobs")

    ls_parser = subparsers.add_parser(
        "ls",
        help="List datasets, versions, or instance IDs"
    )
    ls_parser.add_argument("--name", help="Filter by dataset name")
    ls_parser.add_argument("--version", help="Filter by version")

    get_parser = subparsers.add_parser(
        "get",
        help="Get a specific value by instance ID, name, version, revision, and key"
    )
    get_parser.add_argument("--instance-id", required=True, help="Instance ID to retrieve")
    get_parser.add_argument("--name", required=True, help="Dataset name")
    get_parser.add_argument("--version", required=True, help="Version of the dataset")
    get_parser.add_argument("--key", help="Field/key to retrieve")

    args = parser.parse_args()

    if args.command == "upload":
        handle_upload(args)
    elif args.command == "ls":
        handle_ls(args)
    elif args.command == "get":
        handle_get(args)
    elif args.command == "download":
        handle_download(args)


def get_item_wrapper(args):
    try:
        return get_item(*args), True
    except Exception as e:
        import traceback
        traceback.print_exc()
        return json.dumps({
            "args": args,
            "error": str(e),
            "traceback": traceback.format_exc(),
        }), False


def download_single_dataset(name, version, output, jobs):
    instance_ids = get_all_instance_ids(name, version)
    with multiprocessing.Pool(processes=jobs) as pool:
        pbar = tqdm(pool.imap_unordered(
                    get_item_wrapper,
                    [(name, version, instance_id, None) for instance_id in instance_ids],
                ), total=len(instance_ids))
        with open(output, "w") as f:
            for result, status in pbar:
                if status:
                    f.write(result + "\n")
                else:
                    with open("ossdata-error.jsonl", "a") as f_error:
                        f_error.write(result + "\n")

def download_multiple_datasets(name_with_versions, output, jobs):
    output_root = Path(output)
    os.makedirs(output_root, exist_ok=True)
    meta_path = output_root / "meta.json"
    if meta_path.exists():
        meta = json.loads(meta_path.read_text())
    else:
        meta = []
    for name, version in name_with_versions:
        existing_meta = [x for x in meta if x["name"] == name and x["version"] == version]
        if existing_meta:
            print(f"Dataset '{name}/{version}' is already downloaded. Ignore!")
            continue
        output_path = str((output_root / f"{name}-{version}".replace("/", "-")).resolve()) + ".jsonl"
        download_single_dataset(name, version, output_path, jobs)
        meta.append({
            "name": name,
            "version": version,
            "output": output_path
        })
        (output_root / "meta.json").write_text(json.dumps(meta, indent=4))

def handle_download(args):
    if args.name and args.version:
        download_single_dataset(args.name, args.version, args.output_file, args.j)
        return
    
    name_with_versions = []
    if args.name:
        versions = get_all_versions(args.name)
        for version in versions:
            name_with_versions.append((args.name, version))
    else:
        for name in get_all_datasets():
            versions = get_all_versions(name)
            for version in versions:
                name_with_versions.append((name, version))
    download_multiple_datasets(name_with_versions, args.output_file, args.j)

def upload_single_dataset(ds_arguments, args):

    if "version" in ds_arguments:
        version = ds_arguments["version"]
        if "@" in version:
            split, revision = version.split("@")
        else:
            split, revision = version, None
    else:
        split = ds_arguments["split"]
        version = split
        if ds_arguments.get("revision") is not None:
            revision = ds_arguments["revision"]
            version += f"@{revision}"
        else:
            revision = None

    name = ds_arguments["name"]

    dataset_bucket = OSS_DATASET_PATH
    if len(list_objects(f"{dataset_bucket}/{name}/{version}/")) != 0 and not args.force:
        print(f"Dataset '{name}/{version}' is not empty. Please use --force to update this dataset!")
        exit(-1)
    
    if os.path.isfile(ds_arguments["output"]):
        ds = map(
            lambda x: json.loads(x), 
            filter(
                lambda x: x.strip() != "",
                open(ds_arguments["output"])
            )
        )
        length = None
    else:
        from datasets import load_dataset
        ds = load_dataset(name, split=split, revision=revision)

    if args.j == 1:
        map_func = map
    else:
        pool = multiprocessing.Pool(processes=args.j)
        map_func = pool.imap_unordered
    for _ in tqdm(map_func(
        functools.partial(
            upload_to_oss, 
            name=name,
            split=split, 
            revision=revision, 
            docker_image_prefix=args.docker_image_prefix
        ), ds
    ), total=length):
        pass

# 处理 upload 命令
def handle_upload(args):
    if os.path.isdir(args.input_file):
        upload_list = json.loads((Path(args.input_file) / "meta.json").read_text())
    else:
        assert args.name is not None, "Please specify --name"
        assert args.split is not None, "Please specify --split"
        upload_list = [{
            "name": args.name, 
            "split": args.split, 
            "revision": args.revision,
            "output": args.input_file
        }]
    for x in upload_list:
        upload_single_dataset(x, args)


# 处理 ls 命令（多态：根据参数不同输出不同内容）
def handle_ls(args):
    if args.name is None and args.version is None:
        print("\n".join(get_all_datasets()))

    elif args.name and args.version is None:
        print("\n".join(get_all_versions(args.name)))

    elif args.name and args.version:
        print("\n".join(get_all_instance_ids(args.name, args.version)))
    else:
        print("[ERROR] Invalid combination of ls arguments.", file=sys.stderr)
        sys.exit(1)

# 处理 get 命令
def handle_get(args):
    print(get_item(args.name, args.version, args.instance_id, args.key), end="")

if __name__ == "__main__":
    main()
