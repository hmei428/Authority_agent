#!/usr/bin/env python3
"""
主流程脚本：整合搜索、打分、筛选的完整流程
支持本地文件和OSS两种模式
"""
import argparse
import logging
import os
import time
from datetime import datetime
from typing import List

from config import ApiConfig, OssConfig, PipelineConfig
from search_agent.pipeline import AuthorityAgent
from search_agent.scoring_optimized import default_score_authority, default_score_relevance  # 使用优化版本
from search_agent.search_client import MetaSearchClient
from search_agent.storage import LocalStorageClient, OssStorageClient, default_date_str


def setup_logging(log_dir: str = "./logs") -> logging.Logger:
    """
    配置日志系统
    同时输出到文件和控制台
    """
    os.makedirs(log_dir, exist_ok=True)

    log_filename = os.path.join(
        log_dir,
        f"pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    )

    # 配置日志格式
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(log_filename, encoding="utf-8"),
            logging.StreamHandler(),  # 同时输出到控制台
        ],
    )

    # 过滤第三方库的详细日志，避免刷屏
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("openai").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)

    logger = logging.getLogger(__name__)
    logger.info("=" * 80)
    logger.info("日志文件: %s", log_filename)
    logger.info("=" * 80)

    return logger


def print_statistics(
    logger: logging.Logger,
    agent: AuthorityAgent,
    date_str: str,
    input_paths: List[str],
    elapsed_time: float,
) -> None:
    """
    打印处理统计信息
    """
    # 统计权威性评分分布
    authority_distribution = {1: 0, 2: 0, 3: 0, 4: 0}
    for info in agent.authority_hosts.values():
        score = int(info["authority_score"])
        authority_distribution[score] = authority_distribution.get(score, 0) + 1

    relevance_distribution = agent.relevance_distribution_total

    logger.info("")
    logger.info("=" * 80)
    logger.info("处理统计信息")
    logger.info("=" * 80)
    logger.info("处理日期: %s", date_str)
    logger.info("输入文件数: %d", len(input_paths))
    logger.info("处理耗时: %.2f 秒", elapsed_time)
    logger.info("")
    logger.info("数据处理统计:")
    logger.info("  总query数: %d", agent.stats["total_queries"])
    logger.info("  元搜索成功: %d", agent.stats["search_success"])
    logger.info("  元搜索失败: %d ⚠️", agent.stats["search_failed"])
    logger.info("  权威性打分失败: %d ⚠️", agent.stats["authority_score_failed"])
    logger.info("  相关性打分失败: %d ⚠️", agent.stats["relevance_score_failed"])
    logger.info("  元搜索结果条数: %d", agent.total_metasearch_records)
    logger.info("  总搜索结果数: %d", agent.total_all_results)
    logger.info("")
    logger.info("权威host统计:")
    logger.info("  总数: %d", len(agent.authority_hosts))
    logger.info("  - 1档(极低权威): %d", authority_distribution[1])
    logger.info("  - 2档(一般权威): %d", authority_distribution[2])
    logger.info("  - 3档(中高权威): %d", authority_distribution[3])
    logger.info("  - 4档(顶级权威): %d", authority_distribution[4])
    logger.info("")
    logger.info("相关性评分分布:")
    logger.info("  - 0分(无关): %d", relevance_distribution[0])
    logger.info("  - 1分(弱相关): %d", relevance_distribution[1])
    logger.info("  - 2分(高相关): %d", relevance_distribution[2])
    logger.info("")
    logger.info("高权威高相关结果数: %d", agent.total_qna_records)
    logger.info("=" * 80)
    logger.info("")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="权威内容采集主流程",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例：
  # 本地模式
  python main_pipeline.py \\
    --input-prefix ./data/sample/query_ \\
    --output-dir ./outputs/20251119 \\
    --date 20251119

  # OSS模式
  python main_pipeline.py \\
    --use-oss \\
    --input-prefix oss://bucket/input/queries/query_ \\
    --output-dir ./outputs/20251119 \\
    --date 20251119
        """
    )

    # 输入输出配置
    parser.add_argument(
        "--use-oss",
        action="store_true",
        help="是否使用OSS存储（默认使用本地文件系统）",
    )
    parser.add_argument(
        "--input-prefix",
        required=True,
        help="输入文件路径前缀（本地模式: ./data/query_，OSS模式: oss://bucket/path/query_）",
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
        "--authority-threshold",
        type=int,
        default=2,
        help="权威性阈值，1-4（默认2）",
    )
    parser.add_argument(
        "--relevance-threshold",
        type=int,
        default=1,
        help="相关性阈值，0-2（默认1）",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=8,
        help="并发数（默认8）",
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

    # Checkpoint参数
    parser.add_argument(
        "--checkpoint-interval",
        type=int,
        default=0,
        help="Checkpoint间隔（每N个query保存一次），0表示禁用（默认0）",
    )
    parser.add_argument(
        "--enable-oss-upload",
        action="store_true",
        help="是否启用OSS上传checkpoint文件",
    )
    parser.add_argument(
        "--oss-all-results-path",
        type=str,
        default="",
        help="all_results_with_scores的OSS路径",
    )
    parser.add_argument(
        "--oss-authority-hosts-path",
        type=str,
        default="",
        help="authority_hosts的OSS路径",
    )
    parser.add_argument(
        "--oss-filtered-qna-path",
        type=str,
        default="",
        help="filtered_qna的OSS路径",
    )

    args = parser.parse_args()

    # 配置日志
    logger = setup_logging()

    logger.info("开始执行主流程")
    logger.info("模式: %s", "OSS" if args.use_oss else "本地文件系统")
    logger.info("输入前缀: %s", args.input_prefix)
    logger.info("输出目录: %s", args.output_dir)
    logger.info("处理日期: %s", args.date)

    # 加载配置
    api_config = ApiConfig.from_env()
    api_config.validate()

    pipeline_config = PipelineConfig.from_args(
        topk=args.topk,
        authority_threshold=args.authority_threshold,
        relevance_threshold=args.relevance_threshold,
        max_workers=args.max_workers,
        filter_authority_score=args.filter_authority_score,
        filter_relevance_score=args.filter_relevance_score,
    )

    # 初始化存储客户端
    oss_upload_client = None
    if args.use_oss:
        logger.info("初始化OSS客户端...")
        oss_config = OssConfig.from_env()
        oss_config.validate()
        storage_client = OssStorageClient(
            endpoint=oss_config.endpoint,
            access_key_id=oss_config.access_key_id,
            access_key_secret=oss_config.access_key_secret,
            bucket_name=oss_config.bucket_name,
        )
        logger.info("OSS客户端初始化完成")
        if args.enable_oss_upload:
            oss_upload_client = storage_client
    else:
        logger.info("使用本地文件系统")
        storage_client = LocalStorageClient()
        if args.enable_oss_upload:
            logger.info("为OSS上传单独初始化客户端...")
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

    # 初始化搜索客户端
    logger.info("初始化元搜索客户端...")
    search_client = MetaSearchClient(api_key=api_config.zhipu_api_key)

    # 初始化AuthorityAgent
    logger.info("初始化AuthorityAgent...")
    logger.info("  topk: %d", pipeline_config.topk)
    logger.info("  authority_threshold: %d", pipeline_config.authority_threshold)
    logger.info("  relevance_threshold: %d", pipeline_config.relevance_threshold)
    logger.info("  max_workers: %d", pipeline_config.max_workers)

    # Checkpoint配置
    if args.checkpoint_interval > 0:
        logger.info("Checkpoint配置:")
        logger.info("  checkpoint_interval: %d", args.checkpoint_interval)
        logger.info("  enable_oss_upload: %s", args.enable_oss_upload)
        if args.enable_oss_upload:
            logger.info("  oss_all_results_path: %s", args.oss_all_results_path)
            logger.info("  oss_authority_hosts_path: %s", args.oss_authority_hosts_path)
            logger.info("  oss_filtered_qna_path: %s", args.oss_filtered_qna_path)

    oss_paths = {
        "all_results": args.oss_all_results_path,
        "authority_hosts": args.oss_authority_hosts_path,
        "filtered_qna": args.oss_filtered_qna_path,
    }

    agent = AuthorityAgent(
        search_client=search_client,
        storage_client=storage_client,
        topk=pipeline_config.topk,
        authority_threshold=pipeline_config.authority_threshold,
        relevance_threshold=pipeline_config.relevance_threshold,
        max_workers=pipeline_config.max_workers,
        score_authority=default_score_authority,
        score_relevance=default_score_relevance,
        checkpoint_interval=args.checkpoint_interval,
        output_dir=args.output_dir,
        oss_paths=oss_paths,
        enable_oss_upload=args.enable_oss_upload,
        oss_upload_client=oss_upload_client,
        filter_authority_score=pipeline_config.filter_authority_score,
        filter_relevance_score=pipeline_config.filter_relevance_score,
    )

    # 处理输入文件
    logger.info("")
    logger.info("=" * 80)
    logger.info("开始处理输入文件")
    logger.info("=" * 80)

    start_time = time.time()

    try:
        agent.process_inputs(input_paths)
        elapsed_time = time.time() - start_time

        # 打印统计信息
        print_statistics(logger, agent, args.date, input_paths, elapsed_time)

        # 输出完整的CSV文件（最终结果）
        logger.info("输出完整CSV文件到: %s", args.output_dir)
        agent.flush_outputs_csv(
            output_dir=args.output_dir,
            filter_authority_score=pipeline_config.filter_authority_score,
            filter_relevance_score=pipeline_config.filter_relevance_score,
        )

        logger.info("")
        logger.info("=" * 80)
        logger.info("主流程执行完成！")
        logger.info("=" * 80)

    except KeyboardInterrupt:
        logger.warning("⚠️  用户中断，保存当前进度...")
        if args.checkpoint_interval > 0:
            agent.save_checkpoint(
                filter_authority_score=pipeline_config.filter_authority_score,
                filter_relevance_score=pipeline_config.filter_relevance_score,
            )
        raise
    except Exception as e:
        logger.error("❌ 发生异常: %s", e)
        if args.checkpoint_interval > 0:
            logger.info("保存当前进度...")
            try:
                agent.save_checkpoint(
                    filter_authority_score=pipeline_config.filter_authority_score,
                    filter_relevance_score=pipeline_config.filter_relevance_score,
                )
            except Exception as save_error:
                logger.error("保存进度失败: %s", save_error)
        raise


if __name__ == "__main__":
    main()
