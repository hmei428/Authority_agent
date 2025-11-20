#!/usr/bin/env python3
"""调试脚本：测试元搜索和打分功能"""
import logging
import os

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# 设置环境变量
os.environ["ZHIPU_API_KEY"] = "83834a049770445a912608da03702901"
os.environ["DIRECT_LLM_API_KEY"] = "MAAS680934ffb1a349259ed7beae4272175b"

from search_agent.search_client import MetaSearchClient
from search_agent.scoring import default_score_authority, default_score_relevance

def test_search():
    """测试元搜索"""
    logger.info("=== 测试元搜索 API ===")
    client = MetaSearchClient(api_key=os.environ["ZHIPU_API_KEY"])

    test_query = "驾驶证换证证件照必须白底吗"
    logger.info(f"查询: {test_query}")

    try:
        results = list(client.search(test_query))
        logger.info(f"返回结果数: {len(results)}")

        if results:
            logger.info(f"第一个结果: {results[0]}")
        else:
            logger.warning("元搜索返回空结果！")
    except Exception as e:
        logger.error(f"元搜索失败: {e}", exc_info=True)

def test_scoring():
    """测试权威性和相关性打分"""
    logger.info("\n=== 测试打分功能 ===")

    test_host = "www.gov.cn"
    test_title = "驾驶证换证流程说明"
    test_content = "根据规定，驾驶证换证需要提供白底证件照..."
    test_query = "驾驶证换证证件照必须白底吗"

    logger.info(f"测试host: {test_host}")

    try:
        auth_score = default_score_authority(test_host, test_title, test_content)
        logger.info(f"权威性得分: {auth_score}")
    except Exception as e:
        logger.error(f"权威性打分失败: {e}", exc_info=True)

    try:
        rel_score = default_score_relevance(test_query, test_title, test_content)
        logger.info(f"相关性得分: {rel_score}")
    except Exception as e:
        logger.error(f"相关性打分失败: {e}", exc_info=True)

if __name__ == "__main__":
    test_search()
    test_scoring()
