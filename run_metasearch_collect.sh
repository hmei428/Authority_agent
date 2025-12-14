#!/usr/bin/env bash
# 元搜索采集版：只调用元搜索API，不做LLM打分
# 用于快速采集原始搜索结果

set -euo pipefail

# ========================================
# 配置区域
# ========================================

# 日期配置（默认今天）
DATE="${DATE:-$(date +%Y%m%d)}"
#DATE="20251121"

# 是否使用运行ID（默认true，每次运行独立；设为false支持断点续传）
USE_RUN_ID="${USE_RUN_ID:-true}"

# 运行ID（使用时间戳，确保每次运行独立）
if [ "${USE_RUN_ID}" = "true" ]; then
    RUN_ID="${RUN_ID:-$(date +%H%M%S)}"
else
    RUN_ID=""  # 不使用RUN_ID，支持断点续传
fi

# 元搜索API配置
export ZHIPU_API_KEY="${ZHIPU_API_KEY:-83834a049770445a912608da03702901}"

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
if [ -n "${RUN_ID}" ]; then
    OUTPUT_DIR="${OUTPUT_DIR:-./outputs/${DATE}_${RUN_ID}}"
    LOCAL_OSS_INPUT_DIR="${LOCAL_OSS_INPUT_DIR:-./data/input/oss_${DATE}_${RUN_ID}}"
else
    OUTPUT_DIR="${OUTPUT_DIR:-./outputs/${DATE}}"
    LOCAL_OSS_INPUT_DIR="${LOCAL_OSS_INPUT_DIR:-./data/input/oss_${DATE}}"
fi

# 选择输入要不要从OSS中读取
DOWNLOAD_INPUT_FROM_OSS="${DOWNLOAD_INPUT_FROM_OSS:-true}"
DOWNLOAD_INPUT_USE_DATE="${DOWNLOAD_INPUT_USE_DATE:-false}"
OSS_INPUT_PREFIX="${OSS_INPUT_PREFIX:-oss://xhs-bigdata-swap/user/hadoop/temp_s3/meihaojie_websearch_input_query_agent_parquet_v2_0_4w}"

# 流程参数
TOPK="${TOPK:-10}"                    # 每个query返回的搜索结果数
MAX_WORKERS="${MAX_WORKERS:-32}"      # 并发数

# Checkpoint配置
CHECKPOINT_INTERVAL="${CHECKPOINT_INTERVAL:-1000}"  # 每N个query保存一次
ENABLE_OSS_UPLOAD="${ENABLE_OSS_UPLOAD:-true}"     # 启用OSS上传

# OSS输出路径配置（仅在ENABLE_OSS_UPLOAD=true时生效）
OSS_METASEARCH_OUTPUT_PATH="${OSS_METASEARCH_OUTPUT_PATH:-oss://xhs-bigdata-shequ-search/shequ_algo_mm/websearch_metasearch_result_di/dtm=${DATE}/}"

# ========================================
# 开始执行
# ========================================

echo "========================================================================"
echo "元搜索采集流程 - 无LLM打分版"
echo "========================================================================"
echo "处理日期: ${DATE}"
if [ -n "${RUN_ID}" ]; then
    echo "运行ID: ${RUN_ID} (独立运行模式)"
else
    echo "运行ID: 无 (断点续传模式)"
fi
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
echo "========================================================================"
echo "流程参数:"
echo "  每个query返回结果数 (topk): ${TOPK}"
echo "  并发数: ${MAX_WORKERS}"
echo "========================================================================"
echo "Checkpoint配置:"
echo "  Checkpoint间隔: ${CHECKPOINT_INTERVAL}"
echo "  启用OSS上传: ${ENABLE_OSS_UPLOAD}"
if [ "${ENABLE_OSS_UPLOAD}" = "true" ]; then
  echo "  OSS输出路径: ${OSS_METASEARCH_OUTPUT_PATH}"
fi
echo "========================================================================"
echo ""

# 创建必要的目录
mkdir -p "${OUTPUT_DIR}"
mkdir -p ./logs

# 从OSS下载输入文件
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
echo "开始执行元搜索采集流程..."
echo ""

# 构建基本命令
CMD="python3 collect_metasearch_only.py \
  --input-prefix \"${INPUT_PREFIX}\" \
  --output-dir \"${OUTPUT_DIR}\" \
  --date \"${DATE}\" \
  --topk \"${TOPK}\" \
  --max-workers \"${MAX_WORKERS}\""

# 添加checkpoint参数
if [ "${CHECKPOINT_INTERVAL}" -gt 0 ]; then
  CMD="${CMD} --checkpoint-interval ${CHECKPOINT_INTERVAL}"
fi

# 添加OSS上传参数
if [ "${ENABLE_OSS_UPLOAD}" = "true" ]; then
  CMD="${CMD} --enable-oss-upload"
  CMD="${CMD} --oss-output-path \"${OSS_METASEARCH_OUTPUT_PATH}\""
fi

# 执行命令
eval "${CMD}"

echo ""
echo "========================================================================"
echo "流程执行完成！"
echo "========================================================================"
echo "输出文件："
echo "  ${OUTPUT_DIR}/metasearch_results_<timestamp>.parquet (元搜索原始结果)"
echo ""
echo "日志目录: ./logs"
echo ""
if [ "${CHECKPOINT_INTERVAL}" -gt 0 ]; then
  echo "Checkpoint目录: ${OUTPUT_DIR}/checkpoints (Parquet格式)"
  echo "进度文件: ${OUTPUT_DIR}/progress.json"
  echo ""
fi
if [ "${ENABLE_OSS_UPLOAD}" = "true" ]; then
  echo "OSS输出路径: ${OSS_METASEARCH_OUTPUT_PATH}"
  echo ""
fi
echo "输出格式: query, rank, url, title, content, host, search_engine"
echo "文件格式: Parquet (高压缩率，列式存储)"
echo "========================================================================"
