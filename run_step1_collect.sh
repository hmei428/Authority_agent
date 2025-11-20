#!/usr/bin/env bash
# 第一步：采集搜索结果

set -euo pipefail

# API Key
export ZHIPU_API_KEY="83834a049770445a912608da03702901"

# 输入输出配置
INPUT_FOLDER="${INPUT_FOLDER:-./data/sample}"
OUTPUT_CSV="${OUTPUT_CSV:-./outputs/all_search_results.csv}"
TOPK="${TOPK:-10}"

echo "========================================="
echo "第一步：采集搜索结果"
echo "========================================="
echo "输入文件夹: ${INPUT_FOLDER}"
echo "输出CSV: ${OUTPUT_CSV}"
echo "每个query返回结果数: ${TOPK}"
echo "========================================="

python step1_collect_search_results.py \
  --input-folder "${INPUT_FOLDER}" \
  --output-csv "${OUTPUT_CSV}" \
  --api-key "${ZHIPU_API_KEY}" \
  --topk "${TOPK}"

echo "========================================="
echo "第一步完成！"
echo "输出文件: ${OUTPUT_CSV}"
echo "========================================="
