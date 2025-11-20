#!/usr/bin/env python3
"""
测试脚本：验证 metasearch 和打分服务是否正常
"""
import os
import sys

# 设置环境变量
os.environ["ZHIPU_API_KEY"] = os.getenv("ZHIPU_API_KEY", "83834a049770445a912608da03702901")
os.environ["DIRECT_LLM_API_KEY"] = os.getenv("DIRECT_LLM_API_KEY", "MAAS680934ffb1a349259ed7beae4272175b")

from search_agent.search_client import MetaSearchClient
from search_agent.scoring import default_score_authority, default_score_relevance


def test_metasearch():
    """测试元搜索服务"""
    print("=" * 60)
    print("测试 1: 元搜索服务")
    print("=" * 60)

    try:
        api_key = os.getenv("ZHIPU_API_KEY")
        if not api_key:
            print("❌ ZHIPU_API_KEY 未设置")
            return False

        client = MetaSearchClient(api_key=api_key)
        print(f"✓ MetaSearchClient 初始化成功")

        # 测试搜索
        test_query = "Python编程"
        print(f"\n测试查询: '{test_query}'")
        results = list(client.search(test_query))

        if results:
            print(f"✓ 元搜索成功，返回 {len(results)} 条结果")
            print(f"\n第一条结果示例:")
            print(f"  标题: {results[0].get('title', 'N/A')[:50]}")
            print(f"  链接: {results[0].get('link', 'N/A')[:50]}")
            return True
        else:
            print("❌ 元搜索返回空结果")
            return False

    except Exception as e:
        print(f"❌ 元搜索失败: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_authority_scoring():
    """测试权威性打分服务"""
    print("\n" + "=" * 60)
    print("测试 2: 权威性打分服务")
    print("=" * 60)

    try:
        api_key = os.getenv("DIRECT_LLM_API_KEY")
        if not api_key:
            print("❌ DIRECT_LLM_API_KEY 未设置")
            return False

        print("✓ API Key 已配置")

        # 测试打分
        test_host = "www.python.org"
        test_title = "Python官方网站"
        test_content = "Python是一种广泛使用的编程语言"

        print(f"\n测试host: {test_host}")
        print("调用权威性打分...")
        score = default_score_authority(test_host, test_title, test_content)

        print(f"✓ 权威性打分成功，得分: {score}")
        return True

    except Exception as e:
        print(f"❌ 权威性打分失败: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_relevance_scoring():
    """测试相关性打分服务"""
    print("\n" + "=" * 60)
    print("测试 3: 相关性打分服务")
    print("=" * 60)

    try:
        api_key = os.getenv("DIRECT_LLM_API_KEY")
        if not api_key:
            print("❌ DIRECT_LLM_API_KEY 未设置")
            return False

        print("✓ API Key 已配置")

        # 测试打分
        test_query = "Python编程入门"
        test_title = "Python编程基础教程"
        test_content = "本教程介绍Python编程的基础知识"

        print(f"\n测试query: {test_query}")
        print("调用相关性打分...")
        score = default_score_relevance(test_query, test_title, test_content)

        print(f"✓ 相关性打分成功，得分: {score}")
        return True

    except Exception as e:
        print(f"❌ 相关性打分失败: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    print("\n" + "=" * 60)
    print("开始测试所有服务")
    print("=" * 60)

    results = {
        "metasearch": test_metasearch(),
        "authority": test_authority_scoring(),
        "relevance": test_relevance_scoring(),
    }

    print("\n" + "=" * 60)
    print("测试结果汇总")
    print("=" * 60)
    for name, success in results.items():
        status = "✓ 成功" if success else "✗ 失败"
        print(f"{name:15s}: {status}")

    all_success = all(results.values())
    print("=" * 60)
    if all_success:
        print("✓ 所有服务测试通过！可以开始运行主流程。")
        return 0
    else:
        print("✗ 部分服务测试失败，请检查配置和网络连接。")
        return 1


if __name__ == "__main__":
    sys.exit(main())
