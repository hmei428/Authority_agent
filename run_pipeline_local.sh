#!/usr/bin/env bash
# 本地调试版：使用本地文件，不涉及OSS
# 用于本地测试和调试

set -euo pipefail

# ========================================
# 配置区域
# ========================================

# 日期配置（默认今天）
DATE="${DATE:-$(date +%Y%m%d)}"

# API配置
export ZHIPU_API_KEY="${ZHIPU_API_KEY:-83834a049770445a912608da03702901}"
export DIRECT_LLM_API_KEY="${DIRECT_LLM_API_KEY:-MAAS680934ffb1a349259ed7beae4272175b}"
export DIRECT_LLM_BASE_URL="${DIRECT_LLM_BASE_URL:-http://redservingapi.devops.xiaohongshu.com/v1}"
export AUTHORITY_MODEL="${AUTHORITY_MODEL:-qwen3-30b-a3b}"
export RELEVANCE_MODEL="${RELEVANCE_MODEL:-qwen3-30b-a3b}"

# 输入输出配置
INPUT_PREFIX="${INPUT_PREFIX:-./data/input/query_}"
#INPUT_PREFIX="/Users/meihaojie/Desktop/search_agent/data/input/query_20251120/e2a44915-c39b-11f0-8116-529f74c2253d_0_0_0.parquet"
OUTPUT_DIR="${OUTPUT_DIR:-./outputs/${DATE}}"

# 流程参数
TOPK="${TOPK:-10}"
AUTHORITY_THRESHOLD="${AUTHORITY_THRESHOLD:-2}"
RELEVANCE_THRESHOLD="${RELEVANCE_THRESHOLD:-1}"
MAX_WORKERS="${MAX_WORKERS:-32}"

# 筛选参数（第三个CSV的筛选条件）
FILTER_AUTHORITY_SCORE="${FILTER_AUTHORITY_SCORE:-4}"
FILTER_RELEVANCE_SCORE="${FILTER_RELEVANCE_SCORE:-2}"

# ========================================
# 开始执行
# ========================================

echo "========================================================================"
echo "权威内容采集流程 - 本地调试版"
echo "========================================================================"
echo "处理日期: ${DATE}"
echo "输入前缀: ${INPUT_PREFIX}"
echo "输出目录: ${OUTPUT_DIR}"
echo "========================================================================"
echo "API配置:"
echo "  ZHIPU_API_KEY: ${ZHIPU_API_KEY}"
echo "  DIRECT_LLM_API_KEY: ${DIRECT_LLM_API_KEY}"
echo "  DIRECT_LLM_BASE_URL: ${DIRECT_LLM_BASE_URL}"
echo "  AUTHORITY_MODEL: ${AUTHORITY_MODEL}"
echo "  RELEVANCE_MODEL: ${RELEVANCE_MODEL}"
echo "========================================================================"
echo "流程参数:"
echo "  每个query返回结果数 (topk): ${TOPK}"
echo "  权威性阈值: ${AUTHORITY_THRESHOLD}"
echo "  相关性阈值: ${RELEVANCE_THRESHOLD}"
echo "  并发数: ${MAX_WORKERS}"
echo "  第三个CSV筛选条件: authority_score=${FILTER_AUTHORITY_SCORE}, relevance_score=${FILTER_RELEVANCE_SCORE}"
echo "========================================================================"
echo ""

# 创建必要的目录
mkdir -p "${OUTPUT_DIR}"
mkdir -p ./logs

# 执行主流程
echo "开始执行主流程..."
echo ""

python3 main_pipeline.py \
  --input-prefix "${INPUT_PREFIX}" \
  --output-dir "${OUTPUT_DIR}" \
  --date "${DATE}" \
  --topk "${TOPK}" \
  --authority-threshold "${AUTHORITY_THRESHOLD}" \
  --relevance-threshold "${RELEVANCE_THRESHOLD}" \
  --max-workers "${MAX_WORKERS}" \
  --filter-authority-score "${FILTER_AUTHORITY_SCORE}" \
  --filter-relevance-score "${FILTER_RELEVANCE_SCORE}"

echo ""
echo "========================================================================"
echo "流程执行完成！"
echo "========================================================================"
echo "输出文件："
echo "  0. ${OUTPUT_DIR}/metasearch_results.csv (元搜索原始结果，可用于后续单独打分)"
echo "  1. ${OUTPUT_DIR}/all_results_with_scores.csv (所有结果带评分)"
echo "  2. ${OUTPUT_DIR}/authority_hosts.csv (权威host列表)"
echo "  3. ${OUTPUT_DIR}/filtered_qna.csv (筛选后的高质量结果)"
echo ""
echo "日志目录: ./logs"
echo ""
echo "如需从 metasearch_results.csv 重新打分，可运行："
echo "  python3 score_from_metasearch.py \\"
echo "    --input-csv ${OUTPUT_DIR}/metasearch_results.csv \\"
echo "    --output-dir ${OUTPUT_DIR}/rescore_\$(date +%Y%m%d_%H%M%S) \\"
echo "    --max-workers 32"
echo "========================================================================"
