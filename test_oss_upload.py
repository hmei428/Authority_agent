#!/usr/bin/env python3
"""
模拟 pipeline 的 OSS 上传流程：生成三类 checkpoint parquet，并上传到配置好的 OSS 路径。

用法示例：
  OSS_ENDPOINT=... OSS_ACCESS_KEY_ID=... OSS_ACCESS_KEY_SECRET=... OSS_BUCKET=... \
    OSS_ALL_RESULTS_PATH=oss://bucket/path/all_results/ \
    OSS_AUTHORITY_HOSTS_PATH=oss://bucket/path/hosts/ \
    OSS_FILTERED_QNA_PATH=oss://bucket/path/qna/ \
    python3 test_oss_upload.py --checkpoint-name checkpoint_demo
"""

import argparse
import os
from pathlib import Path
from typing import Dict

import pandas as pd

from config import OssConfig
from search_agent.storage import OssStorageClient


def upload_file(client: OssStorageClient, local_path: Path, oss_base_path: str, checkpoint_name: str) -> None:
    if not oss_base_path:
        print(f"跳过 {local_path.name}，未提供对应的 OSS 路径")
        return

    oss_target = os.path.join(oss_base_path.rstrip("/"), f"{checkpoint_name}.parquet")
    df = pd.read_parquet(local_path)
    client.write_parquet(df, oss_target)
    print(f"上传成功: {local_path.name} → {oss_target}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="测试 OSS 上传是否正常")
    parser.add_argument("--checkpoint-name", default="checkpoint_test", help="写入 OSS 时使用的 checkpoint 名称")
    parser.add_argument(
        "--checkpoint-dir",
        default="/Users/meihaojie/Desktop/search_agent/outputs/20251121/checkpoints/checkpoint_001",
        help="本地 checkpoint 目录（包含三个 parquet 文件）",
    )
    parser.add_argument(
        "--oss-all-results-path",
        default=os.getenv("OSS_ALL_RESULTS_PATH", ""),
        help="all_results_with_scores 的 OSS 目录前缀",
    )
    parser.add_argument(
        "--oss-authority-hosts-path",
        default=os.getenv("OSS_AUTHORITY_HOSTS_PATH", ""),
        help="authority_hosts 的 OSS 目录前缀",
    )
    parser.add_argument(
        "--oss-filtered-qna-path",
        default=os.getenv("OSS_FILTERED_QNA_PATH", ""),
        help="filtered_qna 的 OSS 目录前缀",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    checkpoint_dir = Path(args.checkpoint_dir)
    if not checkpoint_dir.exists():
        raise FileNotFoundError(f"checkpoint 目录不存在: {checkpoint_dir}")

    local_files = {
        "all_results_with_scores": checkpoint_dir / "all_results_with_scores.parquet",
        "authority_hosts": checkpoint_dir / "authority_hosts.parquet",
        "filtered_qna": checkpoint_dir / "filtered_qna.parquet",
    }

    for name, path in local_files.items():
        if not path.exists():
            raise FileNotFoundError(f"缺少 {name} 文件: {path}")

    oss_cfg = OssConfig.from_env()
    oss_cfg.validate()

    client = OssStorageClient(
        endpoint=oss_cfg.endpoint,
        access_key_id=oss_cfg.access_key_id,
        access_key_secret=oss_cfg.access_key_secret,
        bucket_name=oss_cfg.bucket_name,
    )

    upload_file(client, local_files["all_results_with_scores"], args.oss_all_results_path, args.checkpoint_name)
    upload_file(client, local_files["authority_hosts"], args.oss_authority_hosts_path, args.checkpoint_name)
    upload_file(client, local_files["filtered_qna"], args.oss_filtered_qna_path, args.checkpoint_name)

    print("OSS 上传测试完成")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
