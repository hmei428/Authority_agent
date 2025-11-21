#!/usr/bin/env python3
"""
第二步：数据筛选
从大CSV中读取搜索结果，进行权威性和相关性打分，输出两个文件：
1. 权威host列表（host + authority_score）
2. 高权威高相关的query-url-title-content
"""
import argparse
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List

import pandas as pd
from tqdm import tqdm

from search_agent.scoring_optimized import default_score_authority, default_score_relevance

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


def score_and_filter(
    input_csv: str,
    output_authority_csv: str,
    output_qna_csv: str,
    authority_threshold: int = 2,
    relevance_threshold: int = 1,
    max_workers: int = 8,
) -> None:
    """
    从CSV读取搜索结果，进行打分和筛选

    Args:
        input_csv: 输入CSV文件（包含query, url, title, content, host）
        output_authority_csv: 输出权威host的CSV文件
        output_qna_csv: 输出高权威高相关结果的CSV文件
        authority_threshold: 权威性阈值
        relevance_threshold: 相关性阈值
        max_workers: 并发数
    """
    # 1. 读取CSV
    logger.info(f"读取文件: {input_csv}")
    df = pd.read_csv(input_csv)
    logger.info(f"共 {len(df)} 条记录，{df['query'].nunique()} 个查询，{df['host'].nunique()} 个唯一host")

    # 2. 对每个host进行权威性打分
    logger.info("开始对host进行权威性打分...")
    unique_hosts = df["host"].unique()
    authority_scores: Dict[str, int] = {}

    def score_host(host: str) -> tuple:
        """对单个host打分，返回(host, score, reason)"""
        # 取该host的第一条记录的title和content作为样本
        sample = df[df["host"] == host].iloc[0]
        title = sample.get("title", "")
        content = sample.get("content", "")

        try:
            score, reason = default_score_authority(host, title, content)
            return host, score, reason
        except Exception as e:
            logger.warning(f"Host {host} 打分失败: {e}")
            return host, 0, ""

    authority_reasons: Dict[str, str] = {}  # 存储判断依据

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(score_host, host): host for host in unique_hosts}

        for future in tqdm(as_completed(futures), total=len(futures), desc="权威性打分"):
            host, score, reason = future.result()
            authority_scores[host] = score
            authority_reasons[host] = reason

    # 筛选权威host
    authority_hosts = {
        host: score
        for host, score in authority_scores.items()
        if score >= authority_threshold
    }
    logger.info(f"权威host数量: {len(authority_hosts)} / {len(unique_hosts)}")

    # 3. 对权威host的结果进行相关性打分
    logger.info("开始对权威host的结果进行相关性打分...")

    # 只处理权威host的记录
    df_authority = df[df["host"].isin(authority_hosts.keys())].copy()
    logger.info(f"权威host对应的记录数: {len(df_authority)}")

    qna_results: List[Dict] = []

    def score_relevance(row: pd.Series) -> tuple:
        """对单条记录进行相关性打分，返回(row_dict, score, reason)"""
        query = row["query"]
        title = row.get("title", "")
        content = row.get("content", "")

        try:
            score, reason = default_score_relevance(query, title, content)
            return row.to_dict(), score, reason
        except Exception as e:
            logger.warning(f"相关性打分失败 (query={query}): {e}")
            return row.to_dict(), -1, ""

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(score_relevance, row): idx
            for idx, row in df_authority.iterrows()
        }

        for future in tqdm(as_completed(futures), total=len(futures), desc="相关性打分"):
            row_dict, rel_score, rel_reason = future.result()

            if rel_score >= relevance_threshold:
                host = row_dict["host"]
                auth_score = authority_scores.get(host, 0)
                auth_reason = authority_reasons.get(host, "")
                search_engine = row_dict.get("search_engine", "")

                qna_results.append({
                    "query": row_dict["query"],
                    "url": row_dict["url"],
                    "title": row_dict.get("title", ""),
                    "content": row_dict.get("content", ""),
                    "host": host,
                    "search_engine": search_engine,
                    "authority_score": auth_score,
                    "authority_reason": auth_reason,
                    "relevance_score": rel_score,
                    "relevance_reason": rel_reason,
                })

    logger.info(f"高权威高相关记录数: {len(qna_results)}")

    # 4. 保存结果
    # 保存权威host
    df_auth_hosts = pd.DataFrame([
        {"host": host, "authority_score": score}
        for host, score in authority_hosts.items()
    ])
    df_auth_hosts = df_auth_hosts.sort_values("authority_score", ascending=False)
    df_auth_hosts.to_csv(output_authority_csv, index=False, encoding="utf-8-sig")
    logger.info(f"权威host已保存到: {output_authority_csv}")

    # 保存高权威高相关结果
    if qna_results:
        df_qna = pd.DataFrame(qna_results)
        df_qna.to_csv(output_qna_csv, index=False, encoding="utf-8-sig")
        logger.info(f"高权威高相关结果已保存到: {output_qna_csv}")
    else:
        logger.warning("没有符合条件的高权威高相关结果")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="第二步：从搜索结果中筛选权威内容")
    parser.add_argument(
        "--input-csv",
        required=True,
        help="输入CSV文件（第一步生成的搜索结果）",
    )
    parser.add_argument(
        "--output-authority-csv",
        required=True,
        help="输出权威host的CSV文件",
    )
    parser.add_argument(
        "--output-qna-csv",
        required=True,
        help="输出高权威高相关结果的CSV文件",
    )
    parser.add_argument(
        "--authority-threshold",
        type=int,
        default=2,
        help="权威性阈值（默认2）",
    )
    parser.add_argument(
        "--relevance-threshold",
        type=int,
        default=1,
        help="相关性阈值（默认1）",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=8,
        help="并发数（默认8）",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    score_and_filter(
        input_csv=args.input_csv,
        output_authority_csv=args.output_authority_csv,
        output_qna_csv=args.output_qna_csv,
        authority_threshold=args.authority_threshold,
        relevance_threshold=args.relevance_threshold,
        max_workers=args.max_workers,
    )


if __name__ == "__main__":
    main()
