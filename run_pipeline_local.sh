#!/usr/bin/env bash
# 本地调试版：使用本地文件，不涉及OSS
# 用于本地测试和调试

set -euo pipefail

# ========================================
# 配置区域
# ========================================

# 日期配置（默认今天）
#DATE="${DATE:-$(date +%Y%m%d)}"
DATE="20251121"
# API配置
export ZHIPU_API_KEY="${ZHIPU_API_KEY:-83834a049770445a912608da03702901}"
export DIRECT_LLM_API_KEY="${DIRECT_LLM_API_KEY:-MAAS680934ffb1a349259ed7beae4272175b}"
export DIRECT_LLM_BASE_URL="${DIRECT_LLM_BASE_URL:-http://redservingapi.devops.xiaohongshu.com/v1}"
export AUTHORITY_MODEL="${AUTHORITY_MODEL:-qwen3-30b-a3b}"
export RELEVANCE_MODEL="${RELEVANCE_MODEL:-qwen3-30b-a3b}"
# OSS账号配置（输出/上传）
export OSS_ENDPOINT="${OSS_ENDPOINT:-https://oss-cn-shanghai-internal.aliyuncs.com}"
export OSS_ACCESS_KEY_ID="${OSS_ACCESS_KEY_ID:-LTAI4GHNne3HtXvWqsbHX9Gy}"
export OSS_ACCESS_KEY_SECRET="${OSS_ACCESS_KEY_SECRET:-glXyTIUF4Ywv4HOTo9exGW06wgD9Rq}"
export OSS_BUCKET="${OSS_BUCKET:-xhs-bigdata-shequ-search}"

# 输入用的 OSS 账号（可与输出不同）
export INPUT_OSS_ENDPOINT="${INPUT_OSS_ENDPOINT:-$OSS_ENDPOINT}"
export INPUT_OSS_ACCESS_KEY_ID="${INPUT_OSS_ACCESS_KEY_ID:-$OSS_ACCESS_KEY_ID}"
export INPUT_OSS_ACCESS_KEY_SECRET="${INPUT_OSS_ACCESS_KEY_SECRET:-$OSS_ACCESS_KEY_SECRET}"
export INPUT_OSS_BUCKET="${INPUT_OSS_BUCKET:-xhs-bigdata-swap}"

# 输入输出配置
INPUT_PREFIX="${INPUT_PREFIX:-./data/input/query_}"
#INPUT_PREFIX="/Users/meihaojie/Desktop/search_agent/data/input/query_20251120/e2a44915-c39b-11f0-8116-529f74c2253d_0_0_0.parquet"
OUTPUT_DIR="${OUTPUT_DIR:-./outputs/${DATE}}"
#选择输入要不要从oss中读取
DOWNLOAD_INPUT_FROM_OSS="${DOWNLOAD_INPUT_FROM_OSS:-true}"
DOWNLOAD_INPUT_USE_DATE="${DOWNLOAD_INPUT_USE_DATE:-false}"
OSS_INPUT_PREFIX="${OSS_INPUT_PREFIX:-oss://xhs-bigdata-swap/user/hadoop/temp_s3/meihaojie_websearch_test_ll_web_query_tianfeng_add_qwen3biaozhu_forjava_leimu}"
LOCAL_OSS_INPUT_DIR="${LOCAL_OSS_INPUT_DIR:-./data/input/oss_${DATE}}"

# 流程参数
TOPK="${TOPK:-10}"
AUTHORITY_THRESHOLD="${AUTHORITY_THRESHOLD:-1}"
RELEVANCE_THRESHOLD="${RELEVANCE_THRESHOLD:-0}"
MAX_WORKERS="${MAX_WORKERS:-64}"

# 筛选参数（第三个CSV的筛选条件）
FILTER_AUTHORITY_SCORE="${FILTER_AUTHORITY_SCORE:-4}"
FILTER_RELEVANCE_SCORE="${FILTER_RELEVANCE_SCORE:-2}"

# Checkpoint配置
CHECKPOINT_INTERVAL="${CHECKPOINT_INTERVAL:-50}"  # 默认0表示禁用，可设置为50、100等
ENABLE_OSS_UPLOAD="${ENABLE_OSS_UPLOAD:-true}"  # 默认不启用OSS上传

# OSS路径配置（仅在ENABLE_OSS_UPLOAD=true时生效）
# 示例: oss://your-bucket/search-agent/20251120/all_results/
OSS_ALL_RESULTS_PATH="${OSS_ALL_RESULTS_PATH:-oss://xhs-bigdata-shequ-search/shequ_algo_mm/websearch_query_url_agent_full_di/dtm=${DATE}/}"
OSS_AUTHORITY_HOSTS_PATH="${OSS_AUTHORITY_HOSTS_PATH:-oss://xhs-bigdata-shequ-search/shequ_algo_mm/websearch_host_agent_di/dtm=${DATE}/}"
OSS_FILTERED_QNA_PATH="${OSS_FILTERED_QNA_PATH:-oss://xhs-bigdata-shequ-search/shequ_algo_mm/websearch_high_rel_high_auth_agent_di/dtm=${DATE}/}"

# ========================================
# 开始执行
# ========================================

echo "========================================================================"
echo "权威内容采集流程 - 本地调试版"
echo "========================================================================"
echo "处理日期: ${DATE}"
echo "输入前缀: ${INPUT_PREFIX}"
echo "输出目录: ${OUTPUT_DIR}"
echo "从OSS下载输入: ${DOWNLOAD_INPUT_FROM_OSS}"
if [ "${DOWNLOAD_INPUT_FROM_OSS}" = "true" ]; then
echo "  OSS输入前缀: ${OSS_INPUT_PREFIX}"
  echo "  本地输入目录: ${LOCAL_OSS_INPUT_DIR}"
  echo "  按日期筛选: ${DOWNLOAD_INPUT_USE_DATE}"
fi
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
echo "Checkpoint配置:"
echo "  Checkpoint间隔: ${CHECKPOINT_INTERVAL} (0表示禁用)"
echo "  启用OSS上传: ${ENABLE_OSS_UPLOAD}"
if [ "${ENABLE_OSS_UPLOAD}" = "true" ]; then
  echo "  OSS路径:"
  echo "    all_results: ${OSS_ALL_RESULTS_PATH}"
  echo "    authority_hosts: ${OSS_AUTHORITY_HOSTS_PATH}"
  echo "    filtered_qna: ${OSS_FILTERED_QNA_PATH}"
fi
echo "========================================================================"
echo ""

# 创建必要的目录
mkdir -p "${OUTPUT_DIR}"
mkdir -p ./logs

if [ "${DOWNLOAD_INPUT_FROM_OSS}" = "true" ]; then
  mkdir -p "${LOCAL_OSS_INPUT_DIR}"
  echo "开始从OSS下载输入文件..."
  INPUT_OSS_ENDPOINT="${INPUT_OSS_ENDPOINT}" \
  INPUT_OSS_ACCESS_KEY_ID="${INPUT_OSS_ACCESS_KEY_ID}" \
  INPUT_OSS_ACCESS_KEY_SECRET="${INPUT_OSS_ACCESS_KEY_SECRET}" \
  INPUT_OSS_BUCKET="${INPUT_OSS_BUCKET}" \
  python3 download_inputs_from_oss.py \
    --oss-prefix "${OSS_INPUT_PREFIX}" \
    --date "${DATE}" \
    --dest-dir "${LOCAL_OSS_INPUT_DIR}" \
    $([ "${DOWNLOAD_INPUT_USE_DATE}" = "false" ] && echo "--no-date-filter")
  INPUT_PREFIX="${LOCAL_OSS_INPUT_DIR}"
  echo "OSS输入下载完成，新的INPUT_PREFIX=${INPUT_PREFIX}"
fi

# 执行主流程
echo "开始执行主流程..."
echo ""

# 构建基本命令
CMD="python3 main_pipeline.py \
  --input-prefix \"${INPUT_PREFIX}\" \
  --output-dir \"${OUTPUT_DIR}\" \
  --date \"${DATE}\" \
  --topk \"${TOPK}\" \
  --authority-threshold \"${AUTHORITY_THRESHOLD}\" \
  --relevance-threshold \"${RELEVANCE_THRESHOLD}\" \
  --max-workers \"${MAX_WORKERS}\" \
  --filter-authority-score \"${FILTER_AUTHORITY_SCORE}\" \
  --filter-relevance-score \"${FILTER_RELEVANCE_SCORE}\""

# 添加checkpoint参数
if [ "${CHECKPOINT_INTERVAL}" -gt 0 ]; then
  CMD="${CMD} --checkpoint-interval ${CHECKPOINT_INTERVAL}"
fi

# 添加OSS上传参数
if [ "${ENABLE_OSS_UPLOAD}" = "true" ]; then
  CMD="${CMD} --enable-oss-upload"
  CMD="${CMD} --oss-all-results-path \"${OSS_ALL_RESULTS_PATH}\""
  CMD="${CMD} --oss-authority-hosts-path \"${OSS_AUTHORITY_HOSTS_PATH}\""
  CMD="${CMD} --oss-filtered-qna-path \"${OSS_FILTERED_QNA_PATH}\""
fi

# 执行命令
eval "${CMD}"

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
if [ "${CHECKPOINT_INTERVAL}" -gt 0 ]; then
  echo "Checkpoint目录: ${OUTPUT_DIR}/checkpoints"
  echo "进度文件: ${OUTPUT_DIR}/progress.json"
  echo ""
fi
echo "如需从 metasearch_results.csv 重新打分，可运行："
echo "  python3 score_from_metasearch.py \\"
echo "    --input-csv ${OUTPUT_DIR}/metasearch_results.csv \\"
echo "    --output-dir ${OUTPUT_DIR}/rescore_\$(date +%Y%m%d_%H%M%S) \\"
echo "    --max-workers 32"
echo ""
echo "如需启用Checkpoint功能，可设置环境变量："
echo "  CHECKPOINT_INTERVAL=50 ENABLE_OSS_UPLOAD=true \\"
echo "  OSS_ALL_RESULTS_PATH=oss://bucket/path/all_results/ \\"
echo "  OSS_AUTHORITY_HOSTS_PATH=oss://bucket/path/authority_hosts/ \\"
echo "  OSS_FILTERED_QNA_PATH=oss://bucket/path/filtered_qna/ \\"
echo "  bash run_pipeline_local.sh"
echo "========================================================================"
