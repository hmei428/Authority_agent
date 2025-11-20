import ast
import json
import os
import re
import time
from typing import Callable

from openai import OpenAI

# ---- 配置 ----
AUTHORITY_API_KEY = os.getenv("DIRECT_LLM_API_KEY", "")
AUTHORITY_BASE_URL = os.getenv("DIRECT_LLM_BASE_URL", "http://redservingapi.devops.xiaohongshu.com/v1")
AUTHORITY_MODEL = os.getenv("AUTHORITY_MODEL", "qwen3-30b-a3b")
RELEVANCE_API_KEY = os.getenv("DIRECT_LLM_API_KEY", "")
RELEVANCE_BASE_URL = os.getenv("DIRECT_LLM_BASE_URL", "http://redservingapi.devops.xiaohongshu.com/v1")
RELEVANCE_MODEL = os.getenv("RELEVANCE_MODEL", "qwen3-30b-a3b")
MAX_RETRIES = 3
RETRY_DELAY = 1  # seconds

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


def _get_client() -> OpenAI:
    if not AUTHORITY_API_KEY:
        raise RuntimeError("DIRECT_LLM_API_KEY is missing for authority scoring")
    return OpenAI(api_key=AUTHORITY_API_KEY, base_url=AUTHORITY_BASE_URL)

def _get_relevance_client() -> OpenAI:
    if not RELEVANCE_API_KEY:
        raise RuntimeError("DIRECT_LLM_API_KEY is missing for relevance scoring")
    return OpenAI(api_key=RELEVANCE_API_KEY, base_url=RELEVANCE_BASE_URL)


def default_score_authority(host: str, title: str, content: str) -> int:
    """
    调用 DirectLLM 对 Host 进行权威性 1-4 档位打分；失败时返回 0。
    """
    try:
        client = _get_client()
    except Exception:
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
                max_tokens=512,
                temperature=0.1,
            )
            content_resp = completion.choices[0].message.content
            parsed = _parse_json_block(content_resp)
            score = int(parsed.get("标签", 0))
            if score not in (1, 2, 3, 4):
                score = 0
            return score
        except Exception:
            if attempt >= MAX_RETRIES + 1:
                return 0
            time.sleep(RETRY_DELAY)
    return 0


def default_score_relevance(query: str, title: str, content: str) -> int:
    """
    调用 DirectLLM 对 Query-Title-Content 做相关性 0/1/2 打分。
    返回 -1 表示无法判定/错误。
    """
    try:
        client = _get_relevance_client()
    except Exception:
        return -1

    relevance_system_prompt = '''
# 任务说明
任务背景：在搜索与内容匹配场景中，我们需要判断搜索 Query 与候选网页（包含标题和正文内容）之间的语义相关程度。  
你的任务是：你是一个AI助手，负责帮助搜索团队判断给定 Query 与对应网页内容（Title + Content）的相关性强度。  
请依据网页能否有效回答或满足 Query 的搜索意图，将其分为三个等级：0、1、2。

## 输入字段
1. Query：用户搜索词或问题
2. Title：网页标题
3. Content：网页正文内容（可能为空）

## 输出三档
2 高相关 —— 标题与内容能完整、准确地回答或满足 Query 的信息需求。用户阅读该网页后，其问题可被完全解决。  
1 弱相关 —— 标题与内容部分涉及 Query 的主题、背景或相关知识，但不能完整回答问题，仅提供部分参考或片段信息。  
0 无关 —— 标题与内容与 Query 无明显关联，或完全无法提供相关信息，或内容为空、泛泛而谈、偏题。

## 分类标准与示例

### 高相关 (2)
**判断标准：**
- 标题与正文共同能直接回答 Query 提出的问题；
- 内容完整、准确、针对性强；
- 用户无需再查找其他信息即可解决需求。

**正例：**
- Query: “考研数学二大纲”  
  Title: “2025年考研数学二考试大纲完整版下载”  
  Content: “本文提供2025考研数学二考试大纲全文，包含所有章节要求。”  
  → **标签: 2（网页内容完整覆盖问题）**

- Query: “怎么申请澳大利亚留学签证”  
  Title: “澳洲留学签证申请流程详细步骤”  
  Content: “准备材料、填写申请表、提交移民局系统、支付费用、等待结果。”  
  → **标签: 2（完整解答了‘怎么申请’的问题）**

---

### 弱相关 (1)
**判断标准：**
- 内容只覆盖了 Query 主题的一部分；
- 或者内容仅提供背景、案例、部分知识点；
- 用户可能仍需查找其他网页来得到完整答案。

**正例：**
- Query: “怎么申请澳大利亚留学签证”  
  Title: “澳洲留学签证材料清单”  
  Content: “介绍申请签证所需的基本材料，但未提供申请流程。”  
  → **标签: 1（只提供部分相关信息）**

- Query: “新能源车补贴政策”  
  Title: “2025年新能源汽车发展趋势分析”  
  Content: “讨论行业趋势，提及部分政府政策背景，但无具体补贴细节。”  
  → **标签: 1（主题相关但未完整回答）**

---

### 无关 (0)
**判断标准：**
- 标题和内容与 Query 完全无关；
- 或仅包含通用词汇、广告、无实质内容；
- 或内容为空。

**正例：**
- Query: “考研数学二大纲”  
  Title: “2025年艺术生文化课辅导班招生”  
  Content: “我们提供艺考生文化课培训。”  
  → **标签: 0（完全无关）**

- Query: “新能源车补贴政策”  
  Title: “如何选择家用SUV车型”  
  Content: “推荐几款SUV，未涉及政策。”  
  → **标签: 0（无任何相关内容）**

---
**特别提示我发现你容易出现对query的意图识别判断错误 导致对于2档和一档判断不准确的情况 我会给出一些你的badcase希望你可以自我反思优化输出 在你之前的打分中出现了如下的case：
case1:
Query:车祸医疗可以用医保么用的是谁的医保
Title:车祸医保报销可以报销吗,法律,现象普法,好看视频
Content:车祸医保报销可以报销吗 刘伟利律师
你判断出的标签：2
你给出的理由：直接回答医保报销问题
判断错误的原因：不同 query问的是车祸可以使用吗 但是文档回答的是医保应该找谁报销 所以属于部分相关 所以应该是1档
正确的标签应该为：1
---
case2:
Query:贵州省政治理论和常识的赋分
Title:中共中央关于进一步全面深化改革推进中国式现代化的决定
Content:坚持好、发展好、完善好中国新型政党制度。更好发挥党外人士作用，健全党外代表人士队伍建设制度。制定民族团结进步促进法，健全铸牢中华民族共同体意识制度 ...
你判断出的标签：2
你给出的理由：标题与内容直接回答Query
判断错误的原因：因为用户的问题是“贵州省考试科目赋分”，而文本为“中央政治文件”，二者语义场完全不同。
正确的标签应该为：0
---
case3:
Query:没有经营许可的收费犯法吗
Title:无证收停车费是什么罪
Content:法律分析:没有手续的停车场收费应该是违法的,首先,建造停车场就要国家批准.如果没批准属于违章建设,而且还收费,那就属于乱收费啦.法律依据:《中华人民共和国民法典》第一百二十条民事权益受到侵害的,被侵权人有权请... 查看全文 
你判断出的标签：2
你给出的理由：直接解答无证收费的法律定性
判断错误的原因：属于局部解释 用户的问题比较宏观 这里只解决了他问题中的一部分
正确的标签应该为：1
---
case4:
Query:东北团员证是几年级可以拿
Title:北京第二外国语学院2025 级新生入学须知
Content:建议新生到. 双培院校报到时携带本人团员证，根据双培院校团委通知安排具. 体办理团组织关系转接手续。团员证需加盖转出单位印章及注册. 团费交纳日期。 2.所有本科新生的 ...
你判断出的标签：2
你给出的理由：直接回答新生入学要求
判断错误的原因：query问的是什么东北的团员证什么时候可以拿 但是网页回答的是入学须知
正确的标签应该为：0
---
case5:
Query:双十一洗衣液到达拒签有运费吗
Title:国别贸易投资环境报告2008 - 山东省商务厅
Content:为帮助我国企业、相关机构和组织更好地了解我国主要贸易伙伴的贸易和投资政. 策、制度及具体做法，客观认识和掌握国际市场环境，更加积极地参与国际竞争，同时，. 依据WTO有关 ...
你判断出的标签：2
你给出的理由：标题和内容直接对应查询的报告主题
判断错误的原因：url内容和query直接没有关系
正确的标签应该为：0
---
case6:
query:护照还没到手机编码怎么办
title:人在海外,如何开通中国内地手机号码?只有中国护照,无中国身份证或者身份证过期。以及持外籍护照,要申请开通中国手机号码,可以看看这个。_哔哩...
content:您当前的浏览器不支持 HTML5 播放器 请更换浏览器再试试哦~ 投币 稿件举报 记笔记 人在海外,如何开通中国内地手机号码?只有中国护照,无中国身份证或者身份证过期｡以及持外籍护照,要申请开通中国手机号码,可以看看这个｡♦️全新中国手机号码☑️(1)开通方法①在微信(WeChat)中搜索公众号〖易博通eSender〗 ,并且关注这个公众号｡②点击菜单栏〖易博通〗 ③点击〖登记开通服务〗 
你判断出的标签：2
你给出的理由：直接解答开通手机号码方法
判断错误的原因：url和query之间没有关系
正确的标签应该为：0

## 提示
关于打分我希望你严格一点 一定要是明确url的title和content一定可以回答用户的query才可以给两分 
为保证标签体系的可靠性，应坚持“宁可错杀，不可放过”的保守策略。
在打分时，宁可多判为 1，也要确保标为 2 的 URL 质量过硬、与 Query 高度相关，能完整、准确地回答用户问题
## 输出格式
请严格按照以下格式输出：
```json
{
  "标签": 0、1、2,
  "判断依据": "简要说明理由，不超过15个字"
}


'''

    relevance_user_prompt = (
        "请分析 Query 与网页的标题和内容的相关性，输出 0/1/2。\n"
        f"Query: {query}\n"
        f"Title: {title}\n"
        f"Content: {content}\n"
        "务必保守：必须能完整回答才给 2；部分覆盖给 1；无关给 0。"
    )

    messages = [
        {"role": "system", "content": relevance_system_prompt},
        {"role": "user", "content": relevance_user_prompt},
    ]

    for attempt in range(1, MAX_RETRIES + 2):
        try:
            completion = client.chat.completions.create(
                model=RELEVANCE_MODEL,
                messages=messages,
                stream=False,
                max_tokens=512,
                temperature=0.1,
            )
            content_resp = completion.choices[0].message.content
            parsed = _parse_json_block(content_resp)
            score = int(parsed.get("标签", -1))
            if score not in (0, 1, 2):
                score = -1
            return score
        except Exception:
            if attempt >= MAX_RETRIES + 1:
                return -1
            time.sleep(RETRY_DELAY)
    return -1


AuthorityScorer = Callable[[str, str, str], int]
RelevanceScorer = Callable[[str, str, str], int]
