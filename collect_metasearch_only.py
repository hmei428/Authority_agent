#!/usr/bin/env python3
"""
元搜索采集脚本：只调用元搜索API，不做LLM打分
支持checkpoint和断点续传
"""
import argparse
import json
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Dict, List, Optional
from urllib.parse import urlparse

import pandas as pd
from tqdm import tqdm

from config import OssConfig
from search_agent.search_client import MetaSearchClient
from search_agent.storage import LocalStorageClient, OssStorageClient, default_date_str


def setup_logging(log_dir: str = "./logs") -> logging.Logger:
    """配置日志系统"""
    os.makedirs(log_dir, exist_ok=True)

    log_filename = os.path.join(
        log_dir,
        f"metasearch_collect_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    )

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(log_filename, encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )

    # 过滤第三方库的详细日志
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)

    logger = logging.getLogger(__name__)
    logger.info("=" * 80)
    logger.info("日志文件: %s", log_filename)
    logger.info("=" * 80)

    return logger


class MetasearchCollector:
    """元搜索采集器：负责调用元搜索API并保存结果"""

    def __init__(
        self,
        search_client: MetaSearchClient,
        storage_client: LocalStorageClient,
        output_dir: str,
        topk: int = 10,
        max_workers: int = 64,
        checkpoint_interval: int = 100,
        enable_oss_upload: bool = False,
        oss_output_path: str = "",
        oss_upload_client: Optional[OssStorageClient] = None,
    ):
        self.search_client = search_client
        self.storage_client = storage_client
        self.output_dir = output_dir
        self.topk = topk
        self.max_workers = max_workers
        self.checkpoint_interval = checkpoint_interval
        self.enable_oss_upload = enable_oss_upload
        self.oss_output_path = oss_output_path
        self.oss_upload_client = oss_upload_client

        # 数据存储
        self.results: List[Dict] = []
        self.stats = {
            "total_queries": 0,
            "processed_queries": 0,
            "search_success": 0,
            "search_failed": 0,
            "total_results": 0,
        }

        # Checkpoint相关
        self.checkpoint_dir = os.path.join(output_dir, "checkpoints")
        os.makedirs(self.checkpoint_dir, exist_ok=True)
        self.progress_file = os.path.join(output_dir, "progress.json")

        # 生成时间戳（用于文件名，避免覆盖）
        self.timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

        # 日志
        self.logger = logging.getLogger(__name__)

    def load_progress(self) -> int:
        """加载进度文件，返回已处理的query数"""
        if os.path.exists(self.progress_file):
            try:
                with open(self.progress_file, "r", encoding="utf-8") as f:
                    progress = json.load(f)
                    processed = progress.get("processed_queries", 0)
                    self.logger.info("从checkpoint恢复，已处理 %d 个query", processed)
                    return processed
            except Exception as e:
                self.logger.warning("读取进度文件失败: %s，从头开始", e)
        return 0

    def save_progress(self, processed_queries: int) -> None:
        """保存进度"""
        progress = {
            "processed_queries": processed_queries,
            "timestamp": datetime.now().isoformat(),
            "stats": self.stats,
        }
        with open(self.progress_file, "w", encoding="utf-8") as f:
            json.dump(progress, f, ensure_ascii=False, indent=2)

    def fetch_metasearch_results(self, query: str) -> List[Dict]:
        """
        调用元搜索API获取结果
        返回格式: [{"query", "rank", "url", "title", "content", "host", "search_engine"}]
        """
        self.stats["total_queries"] += 1

        try:
            items = self.search_client.search(query)
            self.stats["search_success"] += 1
        except Exception as exc:
            self.stats["search_failed"] += 1
            self.logger.warning("元搜索失败 (query=%s): %s", query, exc)
            return []

        results = []
        for rank, item in enumerate(list(items)[: self.topk], start=1):
            url = item.get("link") or ""
            title = item.get("title") or ""
            content = item.get("content") or ""
            search_engine = item.get("search_engine") or ""
            host = urlparse(url).netloc if url else ""

            if not url or not host:
                continue

            results.append({
                "query": query,
                "rank": rank,
                "url": url,
                "title": title,
                "content": content,
                "host": host,
                "search_engine": search_engine,
            })

        self.stats["total_results"] += len(results)
        return results

    def save_checkpoint(self, start_idx: int, end_idx: int) -> None:
        """保存checkpoint"""
        if not self.results:
            return

        # 保存到本地checkpoint文件（添加时间戳避免覆盖）
        checkpoint_filename = f"metasearch_{start_idx:06d}_{end_idx:06d}_{self.timestamp}.parquet"
        checkpoint_path = os.path.join(self.checkpoint_dir, checkpoint_filename)

        df = pd.DataFrame(self.results)
        df.to_parquet(checkpoint_path, index=False)
        self.logger.info("保存checkpoint: %s (%d条记录)", checkpoint_filename, len(self.results))

        # 上传到OSS
        if self.enable_oss_upload and self.oss_upload_client and self.oss_output_path:
            try:
                oss_key = self.oss_output_path.rstrip("/") + "/checkpoints/" + checkpoint_filename
                # 使用OssStorageClient的write_parquet方法上传
                self.oss_upload_client.write_parquet(df, oss_key)
                self.logger.info("上传checkpoint到OSS: %s", oss_key)
            except Exception as e:
                self.logger.warning("上传checkpoint到OSS失败: %s", e)

        # 保存进度
        self.save_progress(end_idx)

        # 清空当前批次
        self.results = []

    def process_queries(self, queries: List[str]) -> None:
        """处理query列表"""
        # 加载进度
        start_idx = self.load_progress()

        if start_idx >= len(queries):
            self.logger.info("所有query已处理完成")
            return

        queries_to_process = queries[start_idx:]
        self.logger.info("总共 %d 个query，已处理 %d 个，剩余 %d 个",
                        len(queries), start_idx, len(queries_to_process))

        # 使用线程池并发处理
        current_idx = start_idx

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # 提交任务
            future_to_query = {
                executor.submit(self.fetch_metasearch_results, query): (idx, query)
                for idx, query in enumerate(queries_to_process, start=start_idx)
            }

            # 使用tqdm显示进度
            with tqdm(total=len(queries_to_process), desc="处理进度") as pbar:
                for future in as_completed(future_to_query):
                    idx, query = future_to_query[future]
                    try:
                        results = future.result()
                        self.results.extend(results)
                        self.stats["processed_queries"] += 1
                        current_idx = idx + 1

                        # 定期保存checkpoint
                        if self.checkpoint_interval > 0 and \
                           self.stats["processed_queries"] % self.checkpoint_interval == 0:
                            checkpoint_start = current_idx - self.checkpoint_interval
                            self.save_checkpoint(checkpoint_start, current_idx)

                    except Exception as exc:
                        self.logger.error("处理query失败 (query=%s): %s", query, exc)

                    pbar.update(1)

        # 保存剩余的结果
        if self.results:
            checkpoint_start = current_idx - len(self.results)
            self.save_checkpoint(checkpoint_start, current_idx)

    def save_final_results(self) -> str:
        """保存最终完整结果"""
        # 合并所有checkpoint文件
        all_checkpoints = sorted([
            f for f in os.listdir(self.checkpoint_dir)
            if f.startswith("metasearch_") and f.endswith(".parquet")
        ])

        if not all_checkpoints:
            self.logger.warning("没有找到checkpoint文件")
            return ""

        # 读取并合并
        dfs = []
        for checkpoint_file in all_checkpoints:
            checkpoint_path = os.path.join(self.checkpoint_dir, checkpoint_file)
            df = pd.read_parquet(checkpoint_path)
            dfs.append(df)

        final_df = pd.concat(dfs, ignore_index=True)

        # 保存到本地（添加时间戳避免覆盖）
        final_parquet_filename = f"metasearch_results_{self.timestamp}.parquet"
        final_parquet_path = os.path.join(self.output_dir, final_parquet_filename)
        final_df.to_parquet(final_parquet_path, index=False)
        self.logger.info("保存最终结果: %s (%d条记录)", final_parquet_path, len(final_df))

        # 上传到OSS（文件名也包含时间戳）
        if self.enable_oss_upload and self.oss_upload_client and self.oss_output_path:
            try:
                oss_key = self.oss_output_path.rstrip("/") + "/" + final_parquet_filename
                self.oss_upload_client.write_parquet(final_df, oss_key)
                self.logger.info("上传最终结果到OSS: %s", oss_key)
            except Exception as e:
                self.logger.warning("上传最终结果到OSS失败: %s", e)

        return final_parquet_path

    def print_statistics(self) -> None:
        """打印统计信息"""
        self.logger.info("")
        self.logger.info("=" * 80)
        self.logger.info("处理统计信息")
        self.logger.info("=" * 80)
        self.logger.info("总query数: %d", self.stats["total_queries"])
        self.logger.info("已处理query数: %d", self.stats["processed_queries"])
        self.logger.info("元搜索成功: %d", self.stats["search_success"])
        self.logger.info("元搜索失败: %d", self.stats["search_failed"])
        self.logger.info("总结果数: %d", self.stats["total_results"])
        self.logger.info("=" * 80)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="元搜索采集脚本（无LLM打分）",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    # 输入输出配置
    parser.add_argument(
        "--input-prefix",
        required=True,
        help="输入文件路径前缀（本地路径或目录）",
    )
    parser.add_argument(
        "--output-dir",
        required=True,
        help="输出目录（本地路径）",
    )
    parser.add_argument(
        "--date",
        default=default_date_str(),
        help="处理日期（格式: YYYYMMDD，默认今天）",
    )

    # 流程参数
    parser.add_argument(
        "--topk",
        type=int,
        default=10,
        help="每个query返回的搜索结果数（默认10）",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=64,
        help="并发数（默认64）",
    )

    # Checkpoint参数
    parser.add_argument(
        "--checkpoint-interval",
        type=int,
        default=100,
        help="Checkpoint间隔（每N个query保存一次），默认100",
    )
    parser.add_argument(
        "--enable-oss-upload",
        action="store_true",
        help="是否启用OSS上传",
    )
    parser.add_argument(
        "--oss-output-path",
        type=str,
        default="",
        help="OSS输出路径（格式: oss://bucket/path/）",
    )

    args = parser.parse_args()

    # 配置日志
    logger = setup_logging()

    logger.info("开始执行元搜索采集流程")
    logger.info("输入前缀: %s", args.input_prefix)
    logger.info("输出目录: %s", args.output_dir)
    logger.info("处理日期: %s", args.date)

    # 验证API配置
    zhipu_api_key = os.getenv("ZHIPU_API_KEY", "")
    if not zhipu_api_key:
        logger.error("ZHIPU_API_KEY未设置，退出")
        return

    # 初始化存储客户端
    storage_client = LocalStorageClient()

    oss_upload_client = None
    if args.enable_oss_upload:
        logger.info("初始化OSS上传客户端...")
        oss_config = OssConfig.from_env()
        oss_config.validate()
        oss_upload_client = OssStorageClient(
            endpoint=oss_config.endpoint,
            access_key_id=oss_config.access_key_id,
            access_key_secret=oss_config.access_key_secret,
            bucket_name=oss_config.bucket_name,
        )
        logger.info("OSS上传客户端初始化完成")

    # 查找输入文件
    logger.info("查找输入文件: prefix=%s, date=%s", args.input_prefix, args.date)
    input_paths = storage_client.list_parquet_with_date(args.input_prefix, args.date)

    if not input_paths:
        logger.error("未找到输入文件，退出")
        return

    logger.info("找到 %d 个输入文件", len(input_paths))
    for path in input_paths:
        logger.info("  - %s", path)

    # 读取所有query
    logger.info("读取query列表...")
    all_queries = []
    for path in input_paths:
        df = storage_client.read_parquet(path)
        if "query" in df.columns:
            queries = df["query"].dropna().unique().tolist()
            all_queries.extend(queries)
            logger.info("  从 %s 读取 %d 个query", path, len(queries))

    logger.info("总共 %d 个唯一query", len(all_queries))

    if not all_queries:
        logger.error("没有找到query，退出")
        return

    # 初始化元搜索客户端
    logger.info("初始化元搜索客户端...")
    search_client = MetaSearchClient(api_key=zhipu_api_key)

    # 初始化采集器
    logger.info("初始化采集器...")
    logger.info("  topk: %d", args.topk)
    logger.info("  max_workers: %d", args.max_workers)
    logger.info("  checkpoint_interval: %d", args.checkpoint_interval)

    collector = MetasearchCollector(
        search_client=search_client,
        storage_client=storage_client,
        output_dir=args.output_dir,
        topk=args.topk,
        max_workers=args.max_workers,
        checkpoint_interval=args.checkpoint_interval,
        enable_oss_upload=args.enable_oss_upload,
        oss_output_path=args.oss_output_path,
        oss_upload_client=oss_upload_client,
    )

    # 处理query
    logger.info("")
    logger.info("=" * 80)
    logger.info("开始处理query")
    logger.info("=" * 80)

    start_time = time.time()

    try:
        collector.process_queries(all_queries)

        # 保存最终结果
        final_path = collector.save_final_results()

        elapsed_time = time.time() - start_time

        # 打印统计信息
        collector.print_statistics()
        logger.info("处理耗时: %.2f 秒", elapsed_time)
        logger.info("最终输出: %s", final_path)

        logger.info("")
        logger.info("=" * 80)
        logger.info("采集流程执行完成！")
        logger.info("=" * 80)

    except KeyboardInterrupt:
        logger.warning("⚠️  用户中断，保存当前进度...")
        collector.save_progress(collector.stats["processed_queries"])
        raise
    except Exception as e:
        logger.error("❌ 发生异常: %s", e)
        collector.save_progress(collector.stats["processed_queries"])
        raise


if __name__ == "__main__":
    main()
