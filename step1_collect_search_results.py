#!/usr/bin/env python3
"""
第一步：数据采集
从文件夹中读取所有parquet文件，调用元搜索API，输出所有结果到CSV
"""
import argparse
import logging
import os
from pathlib import Path
from typing import List
from urllib.parse import urlparse

import pandas as pd
from tqdm import tqdm

from search_agent.search_client import MetaSearchClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


def collect_search_results(
    input_folder: str,
    output_csv: str,
    api_key: str,
    topk: int = 10,
) -> None:
    """
    从文件夹中读取所有parquet，调用元搜索，输出所有结果到CSV

    Args:
        input_folder: 包含parquet文件的文件夹路径
        output_csv: 输出CSV文件路径
        api_key: 元搜索API key
        topk: 每个query返回的结果数量
    """
    # 1. 查找所有parquet文件
    parquet_files = list(Path(input_folder).glob("*.parquet"))
    logger.info(f"找到 {len(parquet_files)} 个parquet文件")

    if not parquet_files:
        logger.warning(f"文件夹 {input_folder} 中没有找到parquet文件")
        return

    # 2. 初始化元搜索客户端
    search_client = MetaSearchClient(api_key=api_key)

    # 3. 收集所有查询
    all_queries = []
    for pq_file in parquet_files:
        logger.info(f"读取文件: {pq_file}")
        df = pd.read_parquet(pq_file)
        if "query" not in df.columns:
            logger.warning(f"文件 {pq_file} 缺少 'query' 列，跳过")
            continue
        all_queries.extend(df["query"].unique().tolist())

    logger.info(f"共收集到 {len(all_queries)} 个唯一查询")

    # 4. 对每个query调用元搜索，收集结果
    all_results = []

    for query in tqdm(all_queries, desc="采集搜索结果"):
        try:
            search_results = list(search_client.search(query))

            # 取前topk个结果
            for idx, item in enumerate(search_results[:topk], start=1):
                url = item.get("link") or ""
                title = item.get("title") or ""
                content = item.get("content") or ""
                host = urlparse(url).netloc if url else ""

                if not url or not host:
                    continue

                all_results.append({
                    "query": query,
                    "rank": idx,
                    "url": url,
                    "title": title,
                    "content": content,
                    "host": host,
                })

        except Exception as e:
            logger.warning(f"查询 '{query}' 失败: {e}")
            continue

    # 5. 保存到CSV
    if all_results:
        df_results = pd.DataFrame(all_results)
        df_results.to_csv(output_csv, index=False, encoding="utf-8-sig")
        logger.info(f"成功保存 {len(all_results)} 条搜索结果到 {output_csv}")
        logger.info(f"包含 {df_results['query'].nunique()} 个查询，{df_results['host'].nunique()} 个唯一host")
    else:
        logger.warning("没有收集到任何搜索结果")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="第一步：采集搜索结果")
    parser.add_argument(
        "--input-folder",
        required=True,
        help="包含parquet文件的输入文件夹路径",
    )
    parser.add_argument(
        "--output-csv",
        required=True,
        help="输出CSV文件路径",
    )
    parser.add_argument(
        "--api-key",
        default=os.getenv("ZHIPU_API_KEY", ""),
        help="元搜索API Key",
    )
    parser.add_argument(
        "--topk",
        type=int,
        default=10,
        help="每个query返回的结果数量（默认10）",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if not args.api_key:
        raise RuntimeError("需要提供API Key，通过 --api-key 或环境变量 ZHIPU_API_KEY")

    collect_search_results(
        input_folder=args.input_folder,
        output_csv=args.output_csv,
        api_key=args.api_key,
        topk=args.topk,
    )


if __name__ == "__main__":
    main()
