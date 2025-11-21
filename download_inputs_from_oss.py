#!/usr/bin/env python3
"""
从指定的 OSS 目录下载当天的输入 parquet 文件到本地。
"""

import argparse
import os
from pathlib import Path
import sys

from config import OssConfig
from search_agent.storage import OssStorageClient


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download input parquet files from OSS to local directory")
    parser.add_argument("--oss-prefix", required=True, help="OSS 路径前缀，例如 oss://bucket/path/query_")
    parser.add_argument("--date", required=True, help="日期，格式 YYYYMMDD")
    parser.add_argument("--dest-dir", required=True, help="本地存放目录")
    parser.add_argument("--no-date-filter", action="store_true", help="不按日期筛选，下载前缀下所有parquet")
    return parser.parse_args()


def _list_parquet_under_prefix(client: OssStorageClient, prefix: str) -> list[str]:
    base = prefix.rstrip("/")
    if base.startswith("oss://"):
        _, _, rest = base.split("/", 2)
        bucket_name, key_prefix = rest.split("/", 1)
        if bucket_name != client.bucket.bucket_name:
            raise ValueError(f"path bucket {bucket_name} not equal to client bucket {client.bucket.bucket_name}")
    else:
        key_prefix = base
    if key_prefix and not key_prefix.endswith("/"):
        key_prefix = f"{key_prefix}/"
    res = []
    for obj in client.bucket.list_objects(prefix=key_prefix).object_list:
        if obj.key.endswith(".parquet"):
            res.append(f"oss://{client.bucket.bucket_name}/{obj.key}")
    return res


def main() -> int:
    args = parse_args()

    dest_dir = Path(args.dest_dir)
    dest_dir.mkdir(parents=True, exist_ok=True)

    oss_config = OssConfig(
        endpoint=os.getenv("INPUT_OSS_ENDPOINT", os.getenv("OSS_ENDPOINT", "")),
        access_key_id=os.getenv("INPUT_OSS_ACCESS_KEY_ID", os.getenv("OSS_ACCESS_KEY_ID", "")),
        access_key_secret=os.getenv("INPUT_OSS_ACCESS_KEY_SECRET", os.getenv("OSS_ACCESS_KEY_SECRET", "")),
        bucket_name=os.getenv("INPUT_OSS_BUCKET", os.getenv("OSS_BUCKET", "")),
    )
    oss_config.validate()

    client = OssStorageClient(
        endpoint=oss_config.endpoint,
        access_key_id=oss_config.access_key_id,
        access_key_secret=oss_config.access_key_secret,
        bucket_name=oss_config.bucket_name,
    )

    if args.no_date_filter:
        oss_paths = _list_parquet_under_prefix(client, args.oss_prefix)
    else:
        oss_paths = client.list_parquet_with_date(args.oss_prefix, args.date)
    if not oss_paths:
        print(f"未在 {args.oss_prefix} 找到日期 {args.date} 的输入文件", file=sys.stderr)
        return 1

    print(f"准备下载 {len(oss_paths)} 个文件到 {dest_dir}")

    for oss_path in oss_paths:
        key = client._split_bucket_key(oss_path)  # noqa: SLF001
        local_path = dest_dir / os.path.basename(oss_path)
        print(f"⇩ 下载 {oss_path} → {local_path}")
        client.bucket.get_object_to_file(key, str(local_path))

    print("OSS 输入文件下载完成")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
