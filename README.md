# Search Authority Agent

用于每日增量查询的元搜索采集与权威性筛选的离线 Agent。模块化拆分：`search_client` 调用元搜索，`scoring` 负责权威/相关性打分，`storage` 负责本地或 OSS 读写，`pipeline` 串联处理，`agent.py` 作为 CLI。

流程：
- 从指定前缀 + 日期的 Parquet 读取新增 query（支持本地或 OSS）。
- 调用元搜索接口拿到 Top-N 结果（url/title/content）。
- 评估 host 是否权威；收集满足阈值的 host，集中写出。
- 评估结果对 query 的相关性；若 host 权威且相关，则写出 query-url-title-content，供权威回答表入库。

## 快速开始

```bash
python agent.py \
  --input-prefix /path/to/prefix_ \
  --authority-prefix ./outputs/authority \
  --qna-prefix ./outputs/qna \
  --api-key $ZHIPU_API_KEY \
  --date 20240101
```

说明：
- 输入文件名规则 `<prefix><YYYYMMDD>*.parquet`，如 `/data/incremental/query_20240101_part0.parquet`。
- 输出会写到 `<authority-prefix>/<YYYYMMDD>/authority_hosts.parquet` 和 `<qna-prefix>/<YYYYMMDD>/authority_qna.parquet`。
- 可选参数：`--topk`（默认 10）、`--max-workers`（默认 20）、`--authority-threshold`（默认 2）、`--relevance-threshold`（默认 1）。
- `--storage` 可选 `auto|local|oss`，默认 `auto` 根据前缀是否包含 `oss://` 判断。

## 需要补充的部分
- 权威性评分与相关性评分目前是占位函数，需替换为调用内网 LLM 或规则的实现（见 `search_agent/scoring.py`）。
- OSS 访问依赖 `oss2`，请设置 `OSS_ENDPOINT/OSS_ACCESS_KEY_ID/OSS_ACCESS_KEY_SECRET/OSS_BUCKET` 环境变量。
- 线上 join / 写表在本工具之外处理；这里仅产出 parquet。

## 目录结构
- `agent.py`：命令行入口。
- `search_agent/search_client.py`：元搜索调用。
- `search_agent/scoring.py`：权威/相关性打分占位。
- `search_agent/storage.py`：本地与 OSS 读写抽象。
- `search_agent/pipeline.py`：主流程（并发调用、得分聚合、输出）。
- `requirements.txt`：运行依赖。

## 关于元搜索接口
代码直接复用示例中的 zhipu 元搜索接口。API Key 需要通过参数或环境变量传入，避免硬编码。
