#!/usr/bin/env python3
import argparse
import sys
from datasets import load_dataset
from tqdm import tqdm
import functools
import multiprocessing
from ossdata.core import upload_to_oss, get_item, get_all_datasets, get_all_versions, get_all_instance_ids


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
    upload_parser.add_argument("--name", required=True, help="Dataset name (e.g., 'princeton-nlp/SWE-bench')")
    upload_parser.add_argument("--split", required=True, help="Dataset split (e.g., 'test', 'train')")
    upload_parser.add_argument("--docker-image-prefix", help="Docker image prefix for instances")
    upload_parser.add_argument("--revision", help="Optional Hugging Face dataset revision")
    upload_parser.add_argument("-j", type=int, help="Number of parallel jobs")

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


# 处理 upload 命令
def handle_upload(args):
    if args.name.endswith(".jsonl"):
        ds = load_dataset("json", data_files=args.name, split=args.split)
    else:
        ds = load_dataset(args.name, split=args.split, revision=args.revision)
    with multiprocessing.Pool(processes=args.j) as pool:
        for _ in tqdm(pool.imap_unordered(
            functools.partial(
                upload_to_oss, 
                name=args.name,
                split=args.split, 
                revision=args.revision, 
                docker_image_prefix=args.docker_image_prefix
            ), ds
        ), total=len(ds)):
            pass


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
