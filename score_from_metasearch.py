#!/usr/bin/env python3
"""
从 metasearch_results.csv 读取数据并进行大模型打分
优势：
1. 元搜索和打分解耦，避免重复调用元搜索API
2. 可以调整打分参数后重新打分，无需重新搜索
3. 打分速度更快（已有数据，无需等待元搜索）
"""
import argparse
import logging
import os
import time
from datetime import datetime

import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

from search_agent.scoring_optimized import score_both_parallel


def setup_logging(log_dir: str = "./logs") -> logging.Logger:
    """配置日志系统"""
    os.makedirs(log_dir, exist_ok=True)

    log_filename = os.path.join(
        log_dir,
        f"scoring_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    )

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(log_filename, encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )

    # 过滤第三方库日志
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("openai").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)

    logger = logging.getLogger(__name__)
    logger.info("=" * 80)
    logger.info("日志文件: %s", log_filename)
    logger.info("=" * 80)

    return logger


def score_single_row(row: dict, authority_threshold: int, relevance_threshold: int) -> dict:
    """
    对单行数据进行打分（并发安全）
    使用优化后的并行打分函数
    """
    query = row.get("query", "")
    host = row.get("host", "")
    title = row.get("title", "")
    content = row.get("content", "")
    url = row.get("url", "")
    rank = row.get("rank", 0)

    # 优化2：并行打分
    authority_score, relevance_score = score_both_parallel(host, query, title, content)

    return {
        "query": query,
        "rank": rank,
        "url": url,
        "title": title,
        "content": content,
        "host": host,
        "authority_score": authority_score,
        "relevance_score": relevance_score,
    }


def main():
    parser = argparse.ArgumentParser(
        description="从 metasearch_results.csv 读取并进行大模型打分",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "--input-csv",
        required=True,
        help="输入CSV文件路径（metasearch_results.csv）",
    )
    parser.add_argument(
        "--output-dir",
        required=True,
        help="输出目录",
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
        "--filter-authority-score",
        type=int,
        default=4,
        help="第三个CSV筛选条件：权威性评分（默认4）",
    )
    parser.add_argument(
        "--filter-relevance-score",
        type=int,
        default=2,
        help="第三个CSV筛选条件：相关性评分（默认2）",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=32,
        help="并发数（默认32）",
    )

    args = parser.parse_args()

    # 配置日志
    logger = setup_logging()

    logger.info("=" * 80)
    logger.info("从 metasearch_results.csv 进行大模型打分")
    logger.info("=" * 80)
    logger.info("输入文件: %s", args.input_csv)
    logger.info("输出目录: %s", args.output_dir)
    logger.info("权威性阈值: %d", args.authority_threshold)
    logger.info("相关性阈值: %d", args.relevance_threshold)
    logger.info("并发数: %d", args.max_workers)
    logger.info("=" * 80)

    # 读取CSV
    logger.info("读取输入CSV...")
    df = pd.read_csv(args.input_csv)
    logger.info("共 %d 条记录", len(df))

    # 转换为字典列表
    rows = df.to_dict(orient="records")

    # 并发打分
    logger.info("开始并发打分...")
    start_time = time.time()

    all_results_with_scores = []
    authority_hosts = {}
    stats = {
        "total": len(rows),
        "authority_score_failed": 0,
        "relevance_score_failed": 0,
    }

    with ThreadPoolExecutor(max_workers=args.max_workers) as executor:
        futures = [
            executor.submit(score_single_row, row, args.authority_threshold, args.relevance_threshold)
            for row in rows
        ]

        for future in tqdm(as_completed(futures), total=len(futures), desc="打分进度"):
            result = future.result()
            all_results_with_scores.append(result)

            authority_score = result["authority_score"]
            relevance_score = result["relevance_score"]
            host = result["host"]

            # 统计失败
            if authority_score == 0:
                stats["authority_score_failed"] += 1
            if relevance_score == -1:
                stats["relevance_score_failed"] += 1

            # 收集权威host
            if authority_score >= args.authority_threshold:
                existing = authority_hosts.get(host, authority_score)
                authority_hosts[host] = max(existing, authority_score)

    elapsed_time = time.time() - start_time

    # 输出统计
    logger.info("")
    logger.info("=" * 80)
    logger.info("打分完成！")
    logger.info("=" * 80)
    logger.info("总记录数: %d", stats["total"])
    logger.info("处理耗时: %.2f 秒", elapsed_time)
    logger.info("权威性打分失败: %d", stats["authority_score_failed"])
    logger.info("相关性打分失败: %d", stats["relevance_score_failed"])
    logger.info("权威host数: %d", len(authority_hosts))
    logger.info("=" * 80)

    # 输出CSV文件
    os.makedirs(args.output_dir, exist_ok=True)

    # 文件1：所有结果带评分
    df_all = pd.DataFrame(all_results_with_scores)
    csv_path_1 = os.path.join(args.output_dir, "all_results_with_scores.csv")
    df_all.to_csv(csv_path_1, index=False, encoding="utf-8-sig")
    logger.info("✓ 输出文件1: %s (%d 条记录)", csv_path_1, len(df_all))

    # 文件2：权威host列表
    if authority_hosts:
        df_hosts = pd.DataFrame([
            {"host": host, "authority_score": score}
            for host, score in authority_hosts.items()
        ]).sort_values("authority_score", ascending=False)
        csv_path_2 = os.path.join(args.output_dir, "authority_hosts.csv")
        df_hosts.to_csv(csv_path_2, index=False, encoding="utf-8-sig")
        logger.info("✓ 输出文件2: %s (%d 个权威host)", csv_path_2, len(df_hosts))

    # 文件3：筛选后的高质量结果
    filtered_results = [
        {
            "query": rec["query"],
            "url": rec["url"],
            "title": rec["title"],
            "content": rec["content"],
            "authority_score": rec["authority_score"],
            "relevance_score": rec["relevance_score"],
        }
        for rec in all_results_with_scores
        if rec["authority_score"] == args.filter_authority_score
        and rec["relevance_score"] == args.filter_relevance_score
    ]

    if filtered_results:
        df_filtered = pd.DataFrame(filtered_results)
        csv_path_3 = os.path.join(args.output_dir, "filtered_qna.csv")
        df_filtered.to_csv(csv_path_3, index=False, encoding="utf-8-sig")
        logger.info(
            "✓ 输出文件3: %s (%d 条记录, 筛选条件: authority_score=%d, relevance_score=%d)",
            csv_path_3,
            len(df_filtered),
            args.filter_authority_score,
            args.filter_relevance_score,
        )
    else:
        logger.warning(
            "没有符合筛选条件的结果 (authority_score=%d, relevance_score=%d)，跳过文件3",
            args.filter_authority_score,
            args.filter_relevance_score,
        )

    logger.info("")
    logger.info("=" * 80)
    logger.info("所有文件输出完成！")
    logger.info("=" * 80)


if __name__ == "__main__":
    main()
