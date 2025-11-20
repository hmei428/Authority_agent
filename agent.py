import argparse
import logging
import os
from datetime import datetime

from search_agent.pipeline import AuthorityAgent
from search_agent.scoring import default_score_authority, default_score_relevance
from search_agent.search_client import MetaSearchClient
from search_agent.storage import LocalStorageClient, OssStorageClient, default_date_str


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Authority agent for metasearch results")
    parser.add_argument(
        "--input-prefix",
        required=True,
        help="输入 parquet 路径前缀（本地或 oss），文件名规则: <prefix><YYYYMMDD>*.parquet",
    )
    parser.add_argument(
        "--authority-prefix",
        required=True,
        help="权威 host 输出前缀，将在前缀下按日期创建目录并写入 authority_hosts.parquet",
    )
    parser.add_argument(
        "--qna-prefix",
        required=True,
        help="权威且相关结果输出前缀，将在前缀下按日期创建目录并写入 authority_qna.parquet",
    )
    parser.add_argument(
        "--date",
        default=default_date_str(),
        help="处理日期，格式 YYYYMMDD，默认当天",
    )
    parser.add_argument(
        "--api-key",
        default=os.getenv("ZHIPU_API_KEY", ""),
        help="zhipu 元搜索 API Key（可用环境变量 ZHIPU_API_KEY）",
    )
    parser.add_argument("--topk", type=int, default=10)
    parser.add_argument("--authority-threshold", type=int, default=2)
    parser.add_argument("--relevance-threshold", type=int, default=1)
    parser.add_argument("--max-workers", type=int, default=20)
    parser.add_argument(
        "--storage",
        choices=["auto", "local", "oss"],
        default="auto",
        help="存储类型：auto 根据前缀是否包含 oss:// 决定",
    )
    return parser.parse_args()


def make_storage_client(args: argparse.Namespace):
    prefers_oss = args.storage == "oss" or args.input_prefix.startswith("oss://")
    if prefers_oss:
        oss_kwargs = {
            "endpoint": os.getenv("OSS_ENDPOINT", ""),
            "access_key_id": os.getenv("OSS_ACCESS_KEY_ID", ""),
            "access_key_secret": os.getenv("OSS_ACCESS_KEY_SECRET", ""),
            "bucket_name": os.getenv("OSS_BUCKET", ""),
        }
        if not all(oss_kwargs.values()):
            raise RuntimeError(
                "OSS storage selected but env OSS_ENDPOINT/OSS_ACCESS_KEY_ID/OSS_ACCESS_KEY_SECRET/OSS_BUCKET is incomplete"
            )
        return OssStorageClient(**oss_kwargs)
    return LocalStorageClient()


def main() -> None:
    args = parse_args()
    if not args.api_key:
        raise RuntimeError("api-key is required, set --api-key or env ZHIPU_API_KEY")

    # 日期格式校验
    try:
        datetime.strptime(args.date, "%Y%m%d")
    except ValueError as exc:
        raise ValueError("date must be in YYYYMMDD format") from exc

    storage_client = make_storage_client(args)
    input_paths = storage_client.list_parquet_with_date(args.input_prefix, args.date)
    if not input_paths:
        raise FileNotFoundError(
            f"no parquet found with prefix {args.input_prefix} and date {args.date}"
        )

    search_client = MetaSearchClient(api_key=args.api_key)
    agent = AuthorityAgent(
        search_client=search_client,
        storage_client=storage_client,
        topk=args.topk,
        authority_threshold=args.authority_threshold,
        relevance_threshold=args.relevance_threshold,
        max_workers=args.max_workers,
        score_authority=default_score_authority,
        score_relevance=default_score_relevance,
    )

    logger.info("agent initialized, start processing date %s", args.date)
    agent.process_inputs(input_paths)
    agent.flush_outputs(
        authority_prefix=args.authority_prefix,
        qna_prefix=args.qna_prefix,
        date_str=args.date,
    )
    logger.info("agent finished, outputs written")


if __name__ == "__main__":
    main()
