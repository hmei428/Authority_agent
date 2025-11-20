#!/usr/bin/env bash
# 第二步：从搜索结果中筛选权威内容

set -euo pipefail

# API Key
export DIRECT_LLM_API_KEY="MAAS680934ffb1a349259ed7beae4272175b"

# 输入输出配置
INPUT_CSV="${INPUT_CSV:-./outputs/all_search_results.csv}"
OUTPUT_AUTHORITY_CSV="${OUTPUT_AUTHORITY_CSV:-./outputs/authority_hosts.csv}"
OUTPUT_QNA_CSV="${OUTPUT_QNA_CSV:-./outputs/authority_qna.csv}"

# 阈值和并发配置
AUTHORITY_THRESHOLD="${AUTHORITY_THRESHOLD:-2}"
RELEVANCE_THRESHOLD="${RELEVANCE_THRESHOLD:-1}"
MAX_WORKERS="${MAX_WORKERS:-8}"

echo "========================================="
echo "第二步：筛选权威内容"
echo "========================================="
echo "输入CSV: ${INPUT_CSV}"
echo "输出权威host: ${OUTPUT_AUTHORITY_CSV}"
echo "输出高权威高相关: ${OUTPUT_QNA_CSV}"
echo "权威性阈值: ${AUTHORITY_THRESHOLD}"
echo "相关性阈值: ${RELEVANCE_THRESHOLD}"
echo "并发数: ${MAX_WORKERS}"
echo "========================================="

python step2_filter_by_scoring.py \
  --input-csv "${INPUT_CSV}" \
  --output-authority-csv "${OUTPUT_AUTHORITY_CSV}" \
  --output-qna-csv "${OUTPUT_QNA_CSV}" \
  --authority-threshold "${AUTHORITY_THRESHOLD}" \
  --relevance-threshold "${RELEVANCE_THRESHOLD}" \
  --max-workers "${MAX_WORKERS}"

echo "========================================="
echo "第二步完成！"
echo "权威host文件: ${OUTPUT_AUTHORITY_CSV}"
echo "高权威高相关文件: ${OUTPUT_QNA_CSV}"
echo "========================================="
