import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional
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
    ) -> None:
        self.search_client = search_client
        self.storage_client = storage_client
        self.topk = topk
        self.authority_threshold = authority_threshold
        self.relevance_threshold = relevance_threshold
        self.max_workers = max_workers
        self.score_authority = score_authority
        self.score_relevance = score_relevance
        self.authority_hosts: Dict[str, int] = {}
        self.qna_records: List[Dict[str, str]] = []
        self.all_results_with_scores: List[Dict] = []  # 新增：存储所有结果带评分
        self.result_rank_counter: Dict[str, int] = {}  # 新增：追踪每个query的结果排序
        self.metasearch_results: List[Dict] = []  # 新增：存储metasearch原始结果（用于后续单独打分）

        # 统计信息
        self.stats = {
            "total_queries": 0,
            "search_success": 0,
            "search_failed": 0,
            "authority_score_failed": 0,
            "relevance_score_failed": 0,
        }

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
            })

            results.append(
                SearchResult(
                    query=query,
                    query_type=query_type,
                    url=url,
                    title=title,
                    content=content,
                    host=host,
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
            authority_score = self.score_authority(result.host, result.title, result.content)
        except Exception as exc:  # noqa: BLE001
            logger.warning("权威性打分失败 (host=%s): %s", result.host, exc)
            authority_score = 0

        # 对query-content进行相关性打分
        try:
            relevance_score = self.score_relevance(
                result.query, result.title, result.content
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("相关性打分失败 (query=%s): %s", result.query, exc)
            relevance_score = -1

        return {
            "result": result,
            "rank": rank,
            "authority_score": authority_score,
            "relevance_score": relevance_score,
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
        relevance_score = scored["relevance_score"]

        # 更新失败统计
        if authority_score == 0:
            self.stats["authority_score_failed"] += 1
        if relevance_score == -1:
            self.stats["relevance_score_failed"] += 1

        # 存储所有结果（带评分）
        self.all_results_with_scores.append({
            "query": result.query,
            "rank": rank,
            "url": result.url,
            "title": result.title,
            "content": result.content,
            "host": result.host,
            "authority_score": authority_score,
            "relevance_score": relevance_score,
        })

        # 收集权威host
        if authority_score >= self.authority_threshold:
            existing = self.authority_hosts.get(result.host, authority_score)
            self.authority_hosts[result.host] = max(existing, authority_score)

            # 收集高权威高相关的结果
            if relevance_score >= self.relevance_threshold:
                key = (result.query, result.url)
                # 去重同 query-url 组合
                if not any(
                    (rec["query"], rec["url"]) == key for rec in self.qna_records
                ):
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

    def process_dataframe(self, df: pd.DataFrame) -> None:
        from tqdm import tqdm

        if "query" not in df.columns:
            raise ValueError("input missing required column 'query'")
        rows = df.to_dict(orient="records")

        # ========================================
        # 阶段1：并发执行所有query的元搜索
        # ========================================
        logger.info("阶段1: 开始元搜索...")
        all_search_results = []  # 存储所有搜索结果

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            search_futures = {
                executor.submit(
                    self.fetch_results,
                    row.get("query", ""),
                    row.get("type"),
                ): row
                for row in rows
            }

            # 显示元搜索进度
            for future in tqdm(
                as_completed(search_futures),
                total=len(search_futures),
                desc="📡 元搜索进度",
                unit="query",
                leave=True
            ):
                results = future.result()
                if results:
                    all_search_results.extend(results)

        logger.info("✓ 元搜索完成，共获取 %d 条结果", len(all_search_results))

        # ========================================
        # 阶段2：并发对所有结果进行LLM打分
        # ========================================
        logger.info("阶段2: 开始LLM打分...")

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            scoring_futures = [
                executor.submit(self.score_single_result, result, rank)
                for rank, result in enumerate(all_search_results, start=1)
            ]

            # 显示打分进度
            for scoring_future in tqdm(
                as_completed(scoring_futures),
                total=len(scoring_futures),
                desc="🤖 LLM打分进度",
                unit="条",
                leave=True
            ):
                scored = scoring_future.result()
                self._collect_scored_result(scored)

        logger.info("✓ LLM打分完成")

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

        os.makedirs(output_dir, exist_ok=True)

        # 文件0：元搜索原始结果（用于后续单独打分）
        if self.metasearch_results:
            df_metasearch = pd.DataFrame(self.metasearch_results)
            csv_path_0 = os.path.join(output_dir, "metasearch_results.csv")
            df_metasearch.to_csv(csv_path_0, index=False, encoding="utf-8-sig")
            logger.info("✓ 输出文件0: %s (%d 条记录) - 元搜索原始结果", csv_path_0, len(df_metasearch))
        else:
            logger.warning("没有元搜索结果，跳过文件0")

        # 文件1：所有结果带评分
        if self.all_results_with_scores:
            df_all = pd.DataFrame(self.all_results_with_scores)
            csv_path_1 = os.path.join(output_dir, "all_results_with_scores.csv")
            df_all.to_csv(csv_path_1, index=False, encoding="utf-8-sig")
            logger.info("✓ 输出文件1: %s (%d 条记录)", csv_path_1, len(df_all))
        else:
            logger.warning("没有搜索结果，跳过文件1")

        # 文件2：权威host列表
        if self.authority_hosts:
            df_hosts = pd.DataFrame([
                {"host": host, "authority_score": score}
                for host, score in self.authority_hosts.items()
            ]).sort_values("authority_score", ascending=False)
            csv_path_2 = os.path.join(output_dir, "authority_hosts.csv")
            df_hosts.to_csv(csv_path_2, index=False, encoding="utf-8-sig")
            logger.info("✓ 输出文件2: %s (%d 个权威host)", csv_path_2, len(df_hosts))
        else:
            logger.warning("没有权威host，跳过文件2")

        # 文件3：筛选后的高质量结果
        # 筛选条件：authority_score = filter_authority_score 且 relevance_score = filter_relevance_score
        if self.all_results_with_scores:
            filtered_results = [
                {
                    "query": rec["query"],
                    "url": rec["url"],
                    "title": rec["title"],
                    "content": rec["content"],
                    "authority_score": rec["authority_score"],
                    "relevance_score": rec["relevance_score"],
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
