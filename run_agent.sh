#!/usr/bin/env bash
# 启动权威站点采集与相关性打分的示例脚本

set -euo pipefail

# 必填：API Key
export ZHIPU_API_KEY="83834a049770445a912608da03702901"
export DIRECT_LLM_API_KEY="MAAS680934ffb1a349259ed7beae4272175b"

# 可选：如需自定义 LLM / Base URL，请解除注释并填写
# export DIRECT_LLM_BASE_URL="http://redservingapi.devops.xiaohongshu.com/v1"
# export AUTHORITY_MODEL="qwen3-30b-a3b"
# export RELEVANCE_MODEL="qwen3-30b-a3b"

# 输入前缀与日期（默认指向示例文件所在路径前缀）
# 你的文件为 /Users/meihaojie/Desktop/search_agent/data/sample/query_20251118.parquet
INPUT_PREFIX="${INPUT_PREFIX:-/Users/meihaojie/Desktop/search_agent/data/sample/query_}"
DATE_STR="${DATE_STR:-20251118}"

# 输出前缀（本地目录，程序会按日期创建子目录）
AUTHORITY_PREFIX="${AUTHORITY_PREFIX:-./outputs/authority}"
QNA_PREFIX="${QNA_PREFIX:-./outputs/qna}"

# 并发 / TopK / 阈值配置
TOPK="${TOPK:-10}"
MAX_WORKERS="${MAX_WORKERS:-8}"
AUTHORITY_THRESHOLD="${AUTHORITY_THRESHOLD:-2}"
RELEVANCE_THRESHOLD="${RELEVANCE_THRESHOLD:-1}"

python agent.py \
  --input-prefix "${INPUT_PREFIX}" \
  --date "${DATE_STR}" \
  --authority-prefix "${AUTHORITY_PREFIX}" \
  --qna-prefix "${QNA_PREFIX}" \
  --api-key "${ZHIPU_API_KEY}" \
  --topk "${TOPK}" \
  --max-workers "${MAX_WORKERS}" \
  --authority-threshold "${AUTHORITY_THRESHOLD}" \
  --relevance-threshold "${RELEVANCE_THRESHOLD}"
