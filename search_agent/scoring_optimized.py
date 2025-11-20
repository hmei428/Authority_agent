import ast
import hashlib
import json
import logging
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Callable

from openai import OpenAI

logger = logging.getLogger(__name__)

# ---- 配置 ----
AUTHORITY_API_KEY = os.getenv("DIRECT_LLM_API_KEY", "")
AUTHORITY_BASE_URL = os.getenv("DIRECT_LLM_BASE_URL", "http://redservingapi.devops.xiaohongshu.com/v1")
AUTHORITY_MODEL = os.getenv("AUTHORITY_MODEL", "qwen3-30b-a3b")
RELEVANCE_API_KEY = os.getenv("DIRECT_LLM_API_KEY", "")
RELEVANCE_BASE_URL = os.getenv("DIRECT_LLM_BASE_URL", "http://redservingapi.devops.xiaohongshu.com/v1")
RELEVANCE_MODEL = os.getenv("RELEVANCE_MODEL", "qwen3-30b-a3b")
MAX_RETRIES = 2  # 优化3：3 -> 2
RETRY_DELAY = 0.3  # 优化3：1s -> 0.3s

# ---- 全局client复用（优化1：避免每次创建新连接） ----
_authority_client = None
_relevance_client = None

# ---- 缓存（优化1：避免重复打分） ----
_authority_cache = {}  # host -> score
_relevance_cache = {}  # hash(query+title+content) -> score

# ---- 提示词 ----
AUTHORITY_SYSTEM_PROMPT = """
# 任务说明
任务背景：在AI搜索和内容召回中，不同网站（Host）的内容质量、可信度和专业性差异较大。为了帮助搜索团队更好地筛选出高质量、权威的站点，建立可靠的白名单体系，需要你判断输入网站的权威程度等级。

你的任务：依据输入的 URL、Host（输入中仅提供 Host）判断网站在领域内的权威程度等级，分为 1、2、3、4 四个档位。

网站权威程度主要从三个维度评估：
1. 知名度（Popularity）
2. 专业性（Expertise）
3. 可信度（Trustworthiness）

四档含义：
1档 极低权威 —— 垃圾或无可信度网站，内容混乱、死链、盗版、色情、AIGC生成或非法搬运，无被搜索引擎收录。
2档 一般权威 —— 内容较专一但知名度低，通常为个人博客、小众社区或地方站点，有部分原创但无权威背书。
3档 中高权威 —— 领域内有影响力的行业门户或垂直网站，内容体系完整、来源稳定、有一定代表性。
4档 顶级权威 —— 官方或唯一指定来源，具有政府、大学、标准机构等身份，内容权威且不可替代。

要求：只根据 Host 进行判断；如果无法确定，请选择较低档位。

输出格式（必须是 JSON）：
{
  "标签": 1/2/3/4,
  "判断依据": "简要理由，不超过15个字"
}
"""

AUTHORITY_USER_PROMPT = "现在，请你分析Host，给出网站的权威性分档结果。Host: {host}"

# 精简的相关性系统提示词（优化：缩短prompt）
RELEVANCE_SYSTEM_PROMPT = '''
# 任务
判断 Query 与网页（Title + Content）的相关性，分为 0/1/2 三档。

## 三档定义
2 高相关 —— 网页能完整、准确回答 Query，用户无需再查找其他信息。
1 弱相关 —— 网页部分涉及 Query 主题，但不能完整回答，仅提供片段信息。
0 无关 —— 网页与 Query 无关联，或内容为空、泛泛而谈。

## 评分原则
严格保守：必须能完整回答才给 2；部分覆盖给 1；无关给 0。
宁可错杀，不可放过：确保标为 2 的质量过硬。

## 输出格式
```json
{
  "标签": 0/1/2,
  "判断依据": "简要理由，不超过15个字"
}
```
'''

PAT_JSON = re.compile(r"\{.*?\}", re.S)


def _parse_json_block(txt: str) -> dict:
    """
    从文本中提取 JSON 对象，支持 markdown 代码块或原始 JSON。
    """
    seg = txt
    json_start = seg.find("```json")
    if json_start != -1:
        json_start += 7
        json_end = seg.find("```", json_start)
        json_str = seg[json_start:json_end].strip() if json_end != -1 else ""
    else:
        json_str = _extract_json_from_text(seg)

    if not json_str:
        return {}
    try:
        return json.loads(json_str)
    except json.JSONDecodeError:
        try:
            return ast.literal_eval(json_str)
        except Exception:
            return {}


def _extract_json_from_text(text: str) -> str:
    start = text.find("{")
    if start == -1:
        return ""
    brace_count = 0
    for i in range(start, len(text)):
        if text[i] == "{":
            brace_count += 1
        elif text[i] == "}":
            brace_count -= 1
            if brace_count == 0:
                return text[start : i + 1]
    return ""


def _get_authority_client() -> OpenAI:
    """优化1：复用client，避免每次创建新连接"""
    global _authority_client
    if _authority_client is None:
        if not AUTHORITY_API_KEY:
            raise RuntimeError("DIRECT_LLM_API_KEY is missing for authority scoring")
        _authority_client = OpenAI(api_key=AUTHORITY_API_KEY, base_url=AUTHORITY_BASE_URL)
    return _authority_client


def _get_relevance_client() -> OpenAI:
    """优化1：复用client，避免每次创建新连接"""
    global _relevance_client
    if _relevance_client is None:
        if not RELEVANCE_API_KEY:
            raise RuntimeError("DIRECT_LLM_API_KEY is missing for relevance scoring")
        _relevance_client = OpenAI(api_key=RELEVANCE_API_KEY, base_url=RELEVANCE_BASE_URL)
    return _relevance_client


def _make_cache_key(query: str, title: str, content: str) -> str:
    """生成缓存key（hash）"""
    combined = f"{query}|{title}|{content}"
    return hashlib.md5(combined.encode('utf-8')).hexdigest()


def score_authority_cached(host: str, title: str = "", content: str = "") -> int:
    """
    优化1：带缓存的权威性打分
    调用 DirectLLM 对 Host 进行权威性 1-4 档位打分；失败时返回 0。
    """
    # 检查缓存
    if host in _authority_cache:
        return _authority_cache[host]

    try:
        client = _get_authority_client()
    except Exception as e:
        logger.error("创建权威性打分client失败: %s", str(e))
        return 0

    messages = [
        {"role": "system", "content": AUTHORITY_SYSTEM_PROMPT},
        {"role": "user", "content": AUTHORITY_USER_PROMPT.format(host=host)},
    ]

    for attempt in range(1, MAX_RETRIES + 2):
        try:
            completion = client.chat.completions.create(
                model=AUTHORITY_MODEL,
                messages=messages,
                stream=False,
                max_tokens=1024,  # 推理模型需要更多tokens：思考过程 + 最终答案
                temperature=0.1,
            )

            # qwen3-30b-a3b模型返回reasoning_content而不是content
            message = completion.choices[0].message
            content_resp = message.content

            # 如果content为空，尝试使用reasoning_content
            if content_resp is None or content_resp.strip() == "":
                if hasattr(message, 'reasoning_content') and message.reasoning_content:
                    content_resp = message.reasoning_content

            # 检查API是否返回了内容
            if content_resp is None or content_resp.strip() == "":
                logger.warning("权威性打分API返回空内容 (host=%s, 尝试=%d)", host, attempt)
                if attempt >= MAX_RETRIES + 1:
                    logger.error("权威性打分失败 (host=%s, 已重试%d次): API返回空内容", host, MAX_RETRIES)
                    return 0
                time.sleep(RETRY_DELAY)
                continue

            parsed = _parse_json_block(content_resp)
            score = int(parsed.get("标签", 0))
            if score not in (1, 2, 3, 4):
                score = 0

            # 缓存结果
            _authority_cache[host] = score
            return score
        except Exception as e:
            if attempt >= MAX_RETRIES + 1:
                logger.error("权威性打分失败 (host=%s, 已重试%d次): %s", host, MAX_RETRIES, str(e))
                return 0
            time.sleep(RETRY_DELAY)
    return 0


def score_relevance_cached(query: str, title: str, content: str) -> int:
    """
    优化1：带缓存的相关性打分
    调用 DirectLLM 对 Query-Title-Content 做相关性 0/1/2 打分。
    返回 -1 表示无法判定/错误。
    """
    # 检查缓存
    cache_key = _make_cache_key(query, title, content)
    if cache_key in _relevance_cache:
        return _relevance_cache[cache_key]

    try:
        client = _get_relevance_client()
    except Exception as e:
        logger.error("创建相关性打分client失败: %s", str(e))
        return -1

    relevance_user_prompt = (
        "请分析 Query 与网页的标题和内容的相关性，输出 0/1/2。\n"
        f"Query: {query}\n"
        f"Title: {title}\n"
        f"Content: {content}\n"
        "务必保守：必须能完整回答才给 2；部分覆盖给 1；无关给 0。"
    )

    messages = [
        {"role": "system", "content": RELEVANCE_SYSTEM_PROMPT},
        {"role": "user", "content": relevance_user_prompt},
    ]

    for attempt in range(1, MAX_RETRIES + 2):
        try:
            completion = client.chat.completions.create(
                model=RELEVANCE_MODEL,
                messages=messages,
                stream=False,
                max_tokens=1024,  # 推理模型需要更多tokens：思考过程 + 最终答案
                temperature=0.1,
            )

            # qwen3-30b-a3b模型返回reasoning_content而不是content
            message = completion.choices[0].message
            content_resp = message.content

            # 如果content为空，尝试使用reasoning_content
            if content_resp is None or content_resp.strip() == "":
                if hasattr(message, 'reasoning_content') and message.reasoning_content:
                    content_resp = message.reasoning_content

            # 检查API是否返回了内容
            if content_resp is None or content_resp.strip() == "":
                logger.warning("相关性打分API返回空内容 (query=%s, 尝试=%d)", query[:50], attempt)
                if attempt >= MAX_RETRIES + 1:
                    logger.error("相关性打分失败 (query=%s, 已重试%d次): API返回空内容", query[:50], MAX_RETRIES)
                    return -1
                time.sleep(RETRY_DELAY)
                continue

            parsed = _parse_json_block(content_resp)
            score = int(parsed.get("标签", -1))
            if score not in (0, 1, 2):
                score = -1

            # 缓存结果
            _relevance_cache[cache_key] = score
            return score
        except Exception as e:
            if attempt >= MAX_RETRIES + 1:
                logger.error("相关性打分失败 (query=%s, 已重试%d次): %s", query[:50], MAX_RETRIES, str(e))
                return -1
            time.sleep(RETRY_DELAY)
    return -1


def score_both_parallel(host: str, query: str, title: str, content: str) -> tuple:
    """
    优化2：并行执行权威性和相关性打分
    返回 (authority_score, relevance_score)
    """
    with ThreadPoolExecutor(max_workers=2) as executor:
        authority_future = executor.submit(score_authority_cached, host, title, content)
        relevance_future = executor.submit(score_relevance_cached, query, title, content)

        authority_score = authority_future.result()
        relevance_score = relevance_future.result()

    return authority_score, relevance_score


# 类型别名
AuthorityScorer = Callable[[str, str, str], int]
RelevanceScorer = Callable[[str, str, str], int]


# 导出优化后的函数作为默认函数
default_score_authority = score_authority_cached
default_score_relevance = score_relevance_cached
