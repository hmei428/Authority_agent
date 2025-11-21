import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Set, Tuple
from urllib.parse import urlparse

import pandas as pd

from .scoring import AuthorityScorer, RelevanceScorer
from .search_client import MetaSearchClient
from .storage import StorageClient, build_output_path

logger = logging.getLogger(__name__)


@dataclass
class SearchResult:
    query: str
    query_type: Optional[str]
    url: str
    title: str
    content: str
    host: str
    search_engine: str  # 新增：搜索引擎标识


class AuthorityAgent:
    def __init__(
        self,
        search_client: MetaSearchClient,
        storage_client: StorageClient,
        topk: int,
        authority_threshold: int,
        relevance_threshold: int,
        max_workers: int,
        score_authority: AuthorityScorer,
        score_relevance: RelevanceScorer,
        checkpoint_interval: int = 0,  # 新增：checkpoint间隔，0表示不启用
        output_dir: str = "",  # 新增：输出目录
        oss_paths: Optional[Dict[str, str]] = None,  # 新增：OSS路径配置
        enable_oss_upload: bool = False,  # 新增：是否启用OSS上传
        oss_upload_client: Optional[StorageClient] = None,
        filter_authority_score: int = 4,
        filter_relevance_score: int = 2,
    ) -> None:
        self.search_client = search_client
        self.storage_client = storage_client
        self.topk = topk
        self.authority_threshold = authority_threshold
        self.relevance_threshold = relevance_threshold
        self.max_workers = max_workers
        self.score_authority = score_authority
        self.score_relevance = score_relevance
        self.filter_authority_score = filter_authority_score
        self.filter_relevance_score = filter_relevance_score

        # Checkpoint配置
        self.checkpoint_interval = checkpoint_interval
        self.output_dir = output_dir
        self.enable_oss_upload = enable_oss_upload
        self.oss_paths = oss_paths or {}
        self.oss_upload_client = oss_upload_client

        self.authority_hosts: Dict[str, Dict[str, str]] = {}  # 修改：存储 {host: {"authority_score": score, "authority_reason": reason}}
        self.qna_records: List[Dict[str, str]] = []
        self.all_results_with_scores: List[Dict] = []  # 新增：存储所有结果带评分
        self.result_rank_counter: Dict[str, int] = {}  # 新增：追踪每个query的结果排序
        self.metasearch_results: List[Dict] = []  # 新增：存储metasearch原始结果（用于后续单独打分）
        self.authority_hosts_updates: Dict[str, Dict[str, str]] = {}
        self.qna_seen_keys: Set[Tuple[str, str]] = set()
        self.csv_part_index = 0
        self.total_metasearch_records = 0
        self.total_all_results = 0
        self.total_qna_records = 0
        self.relevance_distribution_total = {0: 0, 1: 0, 2: 0}

        # 统计信息
        self.stats = {
            "total_queries": 0,
            "search_success": 0,
            "search_failed": 0,
            "authority_score_failed": 0,
            "relevance_score_failed": 0,
        }

        # Checkpoint相关
        self.checkpoint_count = 0  # checkpoint序号

        if self.checkpoint_interval > 0 and self.output_dir:
            self.checkpoint_dir = os.path.join(self.output_dir, "checkpoints")
            os.makedirs(self.checkpoint_dir, exist_ok=True)
        else:
            self.checkpoint_dir = ""

        self._reset_chunk_state()

    def _reset_chunk_state(self) -> None:
        """清空当前批次缓存，释放内存"""
        self.metasearch_results = []
        self.all_results_with_scores = []
        self.qna_records = []
        self.authority_hosts_updates = {}

    def fetch_results(self, query: str, query_type: Optional[str]) -> List[SearchResult]:
        self.stats["total_queries"] += 1
        try:
            items: Iterable[Dict] = self.search_client.search(query)
            self.stats["search_success"] += 1
        except Exception as exc:  # noqa: BLE001
            self.stats["search_failed"] += 1
            logger.warning("元搜索失败 (query=%s): %s", query, exc)
            return []

        results: List[SearchResult] = []
        for rank, item in enumerate(list(items)[: self.topk], start=1):
            url = item.get("link") or ""
            title = item.get("title") or ""
            content = item.get("content") or ""
            search_engine = item.get("search_engine") or ""
            host = urlparse(url).netloc
            if not url or not host:
                continue

            # 保存到metasearch_results（用于后续单独打分）
            self.metasearch_results.append({
                "query": query,
                "rank": rank,
                "url": url,
                "title": title,
                "content": content,
                "host": host,
                "search_engine": search_engine,
            })
            self.total_metasearch_records += 1

            results.append(
                SearchResult(
                    query=query,
                    query_type=query_type,
                    url=url,
                    title=title,
                    content=content,
                    host=host,
                    search_engine=search_engine,
                )
            )
        return results

    def score_single_result(self, result: SearchResult, rank: int) -> Dict:
        """
        对单个搜索结果进行打分（并发安全）
        只做打分，不修改共享状态
        """
        # 对host进行权威性打分
        try:
            authority_score, authority_reason = self.score_authority(result.host, result.title, result.content)
        except Exception as exc:  # noqa: BLE001
            logger.warning("权威性打分失败 (host=%s): %s", result.host, exc)
            authority_score = 0
            authority_reason = ""

        # 对query-content进行相关性打分
        try:
            relevance_score, relevance_reason = self.score_relevance(
                result.query, result.title, result.content
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("相关性打分失败 (query=%s): %s", result.query, exc)
            relevance_score = -1
            relevance_reason = ""

        return {
            "result": result,
            "rank": rank,
            "authority_score": authority_score,
            "authority_reason": authority_reason,
            "relevance_score": relevance_score,
            "relevance_reason": relevance_reason,
        }

    def evaluate_result(self, result: SearchResult, rank: int) -> None:
        """
        评估单个结果并更新统计（非并发版本，保留用于兼容）
        """
        scored = self.score_single_result(result, rank)
        self._collect_scored_result(scored)

    def _collect_scored_result(self, scored: Dict) -> None:
        """
        收集打分后的结果到共享数据结构（线程安全，需要在主线程调用）
        """
        result = scored["result"]
        rank = scored["rank"]
        authority_score = scored["authority_score"]
        authority_reason = scored["authority_reason"]
        relevance_score = scored["relevance_score"]
        relevance_reason = scored["relevance_reason"]

        # 更新失败统计
        if authority_score == 0:
            self.stats["authority_score_failed"] += 1
        if relevance_score == -1:
            self.stats["relevance_score_failed"] += 1

        # 存储所有结果（带评分和判断依据）
        self.all_results_with_scores.append({
            "query": result.query,
            "rank": rank,
            "url": result.url,
            "title": result.title,
            "content": result.content,
            "host": result.host,
            "search_engine": result.search_engine,
            "authority_score": authority_score,
            "authority_reason": authority_reason,
            "relevance_score": relevance_score,
            "relevance_reason": relevance_reason,
        })
        self.total_all_results += 1
        if relevance_score in self.relevance_distribution_total:
            self.relevance_distribution_total[relevance_score] += 1

        # 收集权威host（同时存储score和reason）
        if authority_score >= self.authority_threshold:
            existing = self.authority_hosts.get(result.host)
            if existing is None or int(existing["authority_score"]) < authority_score:
                host_entry = {
                    "authority_score": str(authority_score),
                    "authority_reason": authority_reason
                }
                self.authority_hosts[result.host] = host_entry
                self.authority_hosts_updates[result.host] = host_entry

            # 收集高权威高相关的结果
            if relevance_score >= self.relevance_threshold:
                key = (result.query, result.url)
                if key not in self.qna_seen_keys:
                    self.qna_seen_keys.add(key)
                    self.qna_records.append(
                        {
                            "query": result.query,
                            "type": result.query_type or "",
                            "url": result.url,
                            "title": result.title,
                            "content": result.content,
                            "authority_score": authority_score,
                            "relevance_score": relevance_score,
                        }
                    )
                    self.total_qna_records += 1

    def save_checkpoint(
        self,
        filter_authority_score: Optional[int] = None,
        filter_relevance_score: Optional[int] = None,
    ) -> None:
        """保存checkpoint到本地并上传OSS"""
        if self.checkpoint_interval <= 0:
            return

        if filter_authority_score is None:
            filter_authority_score = self.filter_authority_score
        if filter_relevance_score is None:
            filter_relevance_score = self.filter_relevance_score

        self.checkpoint_count += 1
        checkpoint_name = f"checkpoint_{self.checkpoint_count:03d}"
        checkpoint_path = os.path.join(self.checkpoint_dir, checkpoint_name)
        os.makedirs(checkpoint_path, exist_ok=True)

        logger.info(f"💾 保存 {checkpoint_name}...")

        try:
            # 1. 保存三个parquet文件到本地
            self._save_checkpoint_parquets(checkpoint_path, filter_authority_score, filter_relevance_score)

            # 2. 上传到OSS（如果启用）
            if self.enable_oss_upload:
                self._upload_checkpoint_to_oss(checkpoint_path, checkpoint_name)

            logger.info(
                "✅ %s 保存完成 (累计处理 query: %d)",
                checkpoint_name,
                self.stats.get("total_queries", 0),
            )

        except Exception as e:
            logger.error(f"❌ 保存checkpoint失败: {e}")
            raise  # 失败则中断处理

    def _save_checkpoint_parquets(self, checkpoint_path: str, filter_authority_score: int, filter_relevance_score: int) -> None:
        """保存三个parquet文件到指定目录"""

        # 文件1: all_results_with_scores.parquet（所有字段转为str）
        if self.all_results_with_scores:
            df_all = pd.DataFrame(self.all_results_with_scores)
            # 将所有字段转换为str类型
            df_all = df_all.astype(str)
            parquet_path = os.path.join(checkpoint_path, "all_results_with_scores.parquet")
            df_all.to_parquet(parquet_path, index=False, engine='pyarrow')
            logger.info(f"  ✓ all_results_with_scores.parquet ({len(df_all)} 条)")
        else:
            logger.warning("  ⚠️  all_results_with_scores 为空，跳过")

        # 文件2: authority_hosts.parquet（添加authority_reason，所有字段转为str）
        if self.authority_hosts_updates:
            df_hosts = pd.DataFrame([
                {
                    "host": host,
                    "authority_score": info["authority_score"],
                    "authority_reason": info["authority_reason"]
                }
                for host, info in self.authority_hosts_updates.items()
            ]).sort_values("authority_score", ascending=False)
            # 将所有字段转换为str类型
            df_hosts = df_hosts.astype(str)
            parquet_path = os.path.join(checkpoint_path, "authority_hosts.parquet")
            df_hosts.to_parquet(parquet_path, index=False, engine='pyarrow')
            logger.info(f"  ✓ authority_hosts.parquet ({len(df_hosts)} 个host)")
        else:
            logger.warning("  ⚠️  本批次 authority_hosts 为空，跳过")

        # 文件3: filtered_qna.parquet（调整字段顺序，所有字段转为str）
        if self.all_results_with_scores:
            filtered_results = [
                {
                    "query": str(rec["query"]),
                    "url": str(rec["url"]),
                    "content": str(rec["content"]),
                    "title": str(rec["title"]),
                    "authority_score": str(rec["authority_score"]),
                    "relevance_score": str(rec["relevance_score"]),
                    "authority_reason": str(rec["authority_reason"]),
                    "relevance_reason": str(rec["relevance_reason"]),
                    "search_engine": str(rec["search_engine"]),
                }
                for rec in self.all_results_with_scores
                if rec["authority_score"] == filter_authority_score
                and rec["relevance_score"] == filter_relevance_score
            ]

            if filtered_results:
                df_filtered = pd.DataFrame(filtered_results)
                parquet_path = os.path.join(checkpoint_path, "filtered_qna.parquet")
                df_filtered.to_parquet(parquet_path, index=False, engine='pyarrow')
                logger.info(f"  ✓ filtered_qna.parquet ({len(df_filtered)} 条)")
            else:
                logger.warning("  ⚠️  filtered_qna 为空（无符合条件的结果）")
        else:
            logger.warning("  ⚠️  all_results为空，跳过filtered_qna")

    def _upload_checkpoint_to_oss(self, checkpoint_path: str, checkpoint_name: str) -> None:
        """上传checkpoint文件到OSS"""
        logger.info(f"☁️  上传 {checkpoint_name} 到OSS...")

        if not self.oss_upload_client:
            raise RuntimeError("启用了OSS上传，但未配置OSS客户端")

        file_mappings = {
            "all_results_with_scores.parquet": self.oss_paths.get("all_results"),
            "authority_hosts.parquet": self.oss_paths.get("authority_hosts"),
            "filtered_qna.parquet": self.oss_paths.get("filtered_qna"),
        }

        for filename, oss_base_path in file_mappings.items():
            if not oss_base_path:
                logger.warning(f"  ⚠️  未配置 {filename} 的OSS路径，跳过上传")
                continue

            local_file = os.path.join(checkpoint_path, filename)
            if not os.path.exists(local_file):
                logger.warning(f"  ⚠️  {filename} 不存在，跳过上传")
                continue

            # 构建OSS完整路径：oss://bucket/path/checkpoint_001.parquet
            oss_file_path = os.path.join(oss_base_path, f"{checkpoint_name}.parquet")

            try:
                # 读取本地parquet
                df = pd.read_parquet(local_file)
                # 使用storage_client上传
                self.oss_upload_client.write_parquet(df, oss_file_path)
                logger.info(f"    ✓ {filename} → {oss_file_path}")
            except Exception as e:
                logger.error(f"    ✗ 上传失败 {filename}: {e}")
                raise  # 上传失败则中断

    def _write_csv_part(self, chunk_index: int) -> None:
        """将当前批次的数据写入分片CSV，并重置缓存"""
        if not any([self.metasearch_results, self.all_results_with_scores, self.qna_records, self.authority_hosts_updates]):
            logger.info("当前批次无数据，跳过CSV输出")
            return

        self.csv_part_index += 1
        part_suffix = f"_part{self.csv_part_index:03d}.csv"
        os.makedirs(self.output_dir, exist_ok=True)
        logger.info("写入CSV分片 #%d (批次索引 %d)", self.csv_part_index, chunk_index)

        # 文件0：metasearch_results
        if self.metasearch_results:
            df_metasearch = pd.DataFrame(self.metasearch_results).astype(str)
            csv_path_0 = os.path.join(self.output_dir, f"metasearch_results{part_suffix}")
            df_metasearch.to_csv(csv_path_0, index=False, encoding="utf-8-sig")
            logger.info("✓ 输出分片文件: %s (%d 条记录)", csv_path_0, len(df_metasearch))
        else:
            logger.info("当前批次无元搜索结果，跳过metasearch CSV")

        # 文件1：所有结果带评分
        if self.all_results_with_scores:
            df_all = pd.DataFrame(self.all_results_with_scores).astype(str)
            csv_path_1 = os.path.join(self.output_dir, f"all_results_with_scores{part_suffix}")
            df_all.to_csv(csv_path_1, index=False, encoding="utf-8-sig")
            logger.info("✓ 输出分片文件: %s (%d 条记录)", csv_path_1, len(df_all))
        else:
            logger.info("当前批次无LLM打分结果，跳过all_results CSV")

        # 文件2：权威host列表（使用本批次更新）
        if self.authority_hosts_updates:
            df_hosts = pd.DataFrame([
                {
                    "host": host,
                    "authority_score": info["authority_score"],
                    "authority_reason": info["authority_reason"],
                }
                for host, info in self.authority_hosts_updates.items()
            ]).sort_values("authority_score", ascending=False)
            df_hosts = df_hosts.astype(str)
            csv_path_2 = os.path.join(self.output_dir, f"authority_hosts{part_suffix}")
            df_hosts.to_csv(csv_path_2, index=False, encoding="utf-8-sig")
            logger.info("✓ 输出分片文件: %s (%d 个host)", csv_path_2, len(df_hosts))
        else:
            logger.info("当前批次无新增权威host，跳过authority_hosts CSV")

        # 文件3：筛选后的高质量结果
        filtered_results = [
            {
                "query": str(rec["query"]),
                "url": str(rec["url"]),
                "content": str(rec["content"]),
                "title": str(rec["title"]),
                "authority_score": str(rec["authority_score"]),
                "relevance_score": str(rec["relevance_score"]),
                "authority_reason": str(rec["authority_reason"]),
                "relevance_reason": str(rec["relevance_reason"]),
                "search_engine": str(rec["search_engine"]),
            }
            for rec in self.all_results_with_scores
            if rec["authority_score"] == self.filter_authority_score
            and rec["relevance_score"] == self.filter_relevance_score
        ]

        if filtered_results:
            df_filtered = pd.DataFrame(filtered_results)
            csv_path_3 = os.path.join(self.output_dir, f"filtered_qna{part_suffix}")
            df_filtered.to_csv(csv_path_3, index=False, encoding="utf-8-sig")
            logger.info(
                "✓ 输出分片文件: %s (%d 条记录, 筛选条件: authority_score=%d, relevance_score=%d)",
                csv_path_3,
                len(df_filtered),
                self.filter_authority_score,
                self.filter_relevance_score,
            )
        else:
            logger.info(
                "当前批次无符合条件的高质量结果 (authority_score=%d, relevance_score=%d)",
                self.filter_authority_score,
                self.filter_relevance_score,
            )

        # 写完后重置批次缓存
        self._reset_chunk_state()

    def process_dataframe(self, df: pd.DataFrame) -> None:
        from tqdm import tqdm

        if "query" not in df.columns:
            raise ValueError("input missing required column 'query'")
        rows = df.to_dict(orient="records")

        if not rows:
            logger.info("✅ 所有query已处理完成！")
            return

        # 将数据按 checkpoint_interval 进行分批处理（0 表示单批处理所有数据）
        chunk_size = self.checkpoint_interval if self.checkpoint_interval > 0 else len(rows)
        chunk_size = max(1, chunk_size)
        total_chunks = (len(rows) + chunk_size - 1) // chunk_size

        for chunk_index, start in enumerate(range(0, len(rows), chunk_size), start=1):
            chunk_rows = rows[start:start + chunk_size]
            if not chunk_rows:
                continue

            self._reset_chunk_state()
            batch_tag = f"(批次 {chunk_index}/{total_chunks})"
            logger.info("阶段1: 开始元搜索 %s，共 %d 个query", batch_tag, len(chunk_rows))
            chunk_search_results: List[SearchResult] = []

            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                search_futures = {
                    executor.submit(
                        self.fetch_results,
                        row.get("query", ""),
                        row.get("type"),
                    ): row
                    for row in chunk_rows
                }

                with tqdm(
                    total=len(chunk_rows),
                    desc=f"📡 元搜索进度 {batch_tag}",
                    unit="query",
                    leave=True,
                ) as pbar:
                    for future in as_completed(search_futures):
                        results = future.result()
                        row = search_futures[future]
                        query = row.get("query", "")

                        if results:
                            chunk_search_results.extend(results)

                        pbar.update(1)

            logger.info("✓ 元搜索完成 %s，共获取 %d 条结果", batch_tag, len(chunk_search_results))

            logger.info("阶段2: 开始LLM打分 %s", batch_tag)
            if not chunk_search_results:
                logger.warning("当前批次没有搜索结果，跳过LLM打分 %s", batch_tag)
            else:
                with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                    scoring_futures = [
                        executor.submit(self.score_single_result, result, rank)
                        for rank, result in enumerate(chunk_search_results, start=1)
                    ]

                    with tqdm(
                        total=len(scoring_futures),
                        desc=f"🤖 LLM打分进度 {batch_tag}",
                        unit="条",
                        leave=True,
                    ) as pbar:
                        for scoring_future in as_completed(scoring_futures):
                            scored = scoring_future.result()
                            self._collect_scored_result(scored)
                            pbar.update(1)

                logger.info("✓ LLM打分完成 %s", batch_tag)

            if not chunk_search_results:
                continue

            if self.checkpoint_interval > 0:
                logger.info("批次处理完成，开始保存Checkpoint %s", batch_tag)
                self.save_checkpoint()

            self._write_csv_part(chunk_index)

    def process_inputs(self, input_paths: List[str]) -> None:
        logger.info("=" * 60)
        logger.info("读取 %d 个parquet文件...", len(input_paths))

        # 读取并合并所有parquet文件
        all_dfs = []
        for path in input_paths:
            df = self.storage_client.read_parquet(path)
            all_dfs.append(df)

        # 合并所有DataFrame
        merged_df = pd.concat(all_dfs, ignore_index=True)
        total_queries = len(merged_df)

        logger.info("✓ 读取完成: %d 个文件, 共 %d 条query", len(input_paths), total_queries)
        logger.info("=" * 60)
        logger.info("")

        # 对所有query进行metasearch和LLM打分
        self.process_dataframe(merged_df)

        logger.info("")
        logger.info("=" * 60)
        logger.info("所有处理完成！")
        logger.info("=" * 60)

    def flush_outputs(self, authority_prefix: str, qna_prefix: str, date_str: str) -> None:
        if self.csv_part_index > 0:
            logger.info("已按批输出CSV/Parquet分片，flush_outputs跳过汇总输出")
            return

        if authority_prefix:
            authority_path = build_output_path(authority_prefix, date_str, "authority_hosts.parquet")
            df_hosts = pd.DataFrame(
                [
                    {"host": host, "authority_score": score}
                    for host, score in self.authority_hosts.items()
                ]
            )
            self.storage_client.write_parquet(df_hosts, authority_path)
            logger.info("authority hosts written to %s", authority_path)

        if qna_prefix:
            qna_path = build_output_path(qna_prefix, date_str, "authority_qna.parquet")
            df_qna = pd.DataFrame(self.qna_records)
            self.storage_client.write_parquet(df_qna, qna_path)
            logger.info("authority qna written to %s", qna_path)

    def flush_outputs_csv(
        self,
        output_dir: str,
        filter_authority_score: int = 4,
        filter_relevance_score: int = 2,
    ) -> None:
        """
        输出4个CSV文件：
        0. metasearch_results.csv - 元搜索原始结果（可用于后续单独打分）
        1. all_results_with_scores.csv - 所有结果带评分
        2. authority_hosts.csv - 权威host列表
        3. filtered_qna.csv - 筛选后的高质量结果（authority_score=filter_authority_score 且 relevance_score=filter_relevance_score）
        """
        import os

        if self.csv_part_index > 0:
            logger.info(
                "已输出 %d 个CSV分片 (metasearch_results_partXXX.csv 等)，跳过最终汇总",
                self.csv_part_index,
            )
            return

        os.makedirs(output_dir, exist_ok=True)

        # 文件0：元搜索原始结果（用于后续单独打分，所有字段转为str）
        if self.metasearch_results:
            df_metasearch = pd.DataFrame(self.metasearch_results)
            df_metasearch = df_metasearch.astype(str)
            csv_path_0 = os.path.join(output_dir, "metasearch_results.csv")
            df_metasearch.to_csv(csv_path_0, index=False, encoding="utf-8-sig")
            logger.info("✓ 输出文件0: %s (%d 条记录) - 元搜索原始结果", csv_path_0, len(df_metasearch))
        else:
            logger.warning("没有元搜索结果，跳过文件0")

        # 文件1：所有结果带评分（所有字段转为str）
        if self.all_results_with_scores:
            df_all = pd.DataFrame(self.all_results_with_scores)
            df_all = df_all.astype(str)
            csv_path_1 = os.path.join(output_dir, "all_results_with_scores.csv")
            df_all.to_csv(csv_path_1, index=False, encoding="utf-8-sig")
            logger.info("✓ 输出文件1: %s (%d 条记录)", csv_path_1, len(df_all))
        else:
            logger.warning("没有搜索结果，跳过文件1")

        # 文件2：权威host列表（添加authority_reason，所有字段转为str）
        if self.authority_hosts:
            df_hosts = pd.DataFrame([
                {
                    "host": host,
                    "authority_score": info["authority_score"],
                    "authority_reason": info["authority_reason"]
                }
                for host, info in self.authority_hosts.items()
            ]).sort_values("authority_score", ascending=False)
            df_hosts = df_hosts.astype(str)
            csv_path_2 = os.path.join(output_dir, "authority_hosts.csv")
            df_hosts.to_csv(csv_path_2, index=False, encoding="utf-8-sig")
            logger.info("✓ 输出文件2: %s (%d 个权威host)", csv_path_2, len(df_hosts))
        else:
            logger.warning("没有权威host，跳过文件2")

        # 文件3：筛选后的高质量结果（调整字段顺序，所有字段转为str）
        # 筛选条件：authority_score = filter_authority_score 且 relevance_score = filter_relevance_score
        if self.all_results_with_scores:
            filtered_results = [
                {
                    "query": str(rec["query"]),
                    "url": str(rec["url"]),
                    "content": str(rec["content"]),
                    "title": str(rec["title"]),
                    "authority_score": str(rec["authority_score"]),
                    "relevance_score": str(rec["relevance_score"]),
                    "authority_reason": str(rec["authority_reason"]),
                    "relevance_reason": str(rec["relevance_reason"]),
                    "search_engine": str(rec["search_engine"]),
                }
                for rec in self.all_results_with_scores
                if rec["authority_score"] == filter_authority_score
                and rec["relevance_score"] == filter_relevance_score
            ]

            if filtered_results:
                df_filtered = pd.DataFrame(filtered_results)
                csv_path_3 = os.path.join(output_dir, "filtered_qna.csv")
                df_filtered.to_csv(csv_path_3, index=False, encoding="utf-8-sig")
                logger.info(
                    "✓ 输出文件3: %s (%d 条记录, 筛选条件: authority_score=%d, relevance_score=%d)",
                    csv_path_3,
                    len(df_filtered),
                    filter_authority_score,
                    filter_relevance_score,
                )
            else:
                logger.warning(
                    "没有符合筛选条件的结果 (authority_score=%d, relevance_score=%d)，跳过文件3",
                    filter_authority_score,
                    filter_relevance_score,
                )
        else:
            logger.warning("没有搜索结果，跳过文件3")
