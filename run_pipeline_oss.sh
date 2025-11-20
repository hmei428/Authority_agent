#!/usr/bin/env bash
# 完整版：从OSS读取 -> 处理 -> 上传到OSS
# 用于生产环境，由外部平台定时触发

set -euo pipefail

# ========================================
# 配置区域
# ========================================

# 日期配置（默认今天）
DATE="${DATE:-$(date +%Y%m%d)}"

# OSS配置
export OSS_ENDPOINT="${OSS_ENDPOINT:-oss-cn-shanghai.aliyuncs.com}"
export OSS_ACCESS_KEY_ID="${OSS_ACCESS_KEY_ID:-YOUR_ACCESS_KEY_ID}"
export OSS_ACCESS_KEY_SECRET="${OSS_ACCESS_KEY_SECRET:-YOUR_ACCESS_KEY_SECRET}"
export OSS_BUCKET="${OSS_BUCKET:-your-bucket-name}"

# OSS路径配置
OSS_INPUT_PREFIX="${OSS_INPUT_PREFIX:-input/queries/query_}"
OSS_OUTPUT_PREFIX="${OSS_OUTPUT_PREFIX:-output/results}"

# API配置
export ZHIPU_API_KEY="${ZHIPU_API_KEY:-YOUR_ZHIPU_API_KEY}"
export DIRECT_LLM_API_KEY="${DIRECT_LLM_API_KEY:-YOUR_LLM_API_KEY}"
export DIRECT_LLM_BASE_URL="${DIRECT_LLM_BASE_URL:-http://redservingapi.devops.xiaohongshu.com/v1}"
export AUTHORITY_MODEL="${AUTHORITY_MODEL:-qwen3-30b-a3b}"
export RELEVANCE_MODEL="${RELEVANCE_MODEL:-qwen3-30b-a3b}"

# 本地临时目录
OUTPUT_DIR="./outputs/${DATE}"

# 流程参数
TOPK="${TOPK:-10}"
AUTHORITY_THRESHOLD="${AUTHORITY_THRESHOLD:-2}"
RELEVANCE_THRESHOLD="${RELEVANCE_THRESHOLD:-1}"
MAX_WORKERS="${MAX_WORKERS:-8}"

# 筛选参数（第三个CSV的筛选条件）
FILTER_AUTHORITY_SCORE="${FILTER_AUTHORITY_SCORE:-4}"
FILTER_RELEVANCE_SCORE="${FILTER_RELEVANCE_SCORE:-2}"

# ========================================
# 开始执行
# ========================================

echo "========================================================================"
echo "权威内容采集流程 - 完整版（含OSS）"
echo "========================================================================"
echo "处理日期: ${DATE}"
echo "OSS Bucket: ${OSS_BUCKET}"
echo "OSS输入前缀: ${OSS_INPUT_PREFIX}"
echo "OSS输出前缀: ${OSS_OUTPUT_PREFIX}"
echo "本地输出目录: ${OUTPUT_DIR}"
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

# 执行主流程（从OSS读取）
echo "步骤1: 从OSS读取parquet文件并处理..."
echo ""

python main_pipeline.py \
  --use-oss \
  --input-prefix "oss://${OSS_BUCKET}/${OSS_INPUT_PREFIX}" \
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
echo "步骤2: 上传CSV文件到OSS..."
echo "========================================================================"

# 上传3个CSV文件到OSS
python3 - <<EOF
import os
import sys
from config import OssConfig

# 初始化OSS客户端
oss_config = OssConfig.from_env()
oss_config.validate()

try:
    import oss2
except ImportError:
    print("错误: 未安装oss2，请运行: pip install oss2")
    sys.exit(1)

auth = oss2.Auth(oss_config.access_key_id, oss_config.access_key_secret)
bucket = oss2.Bucket(auth, oss_config.endpoint, oss_config.bucket_name)

# 定义要上传的文件
output_dir = "${OUTPUT_DIR}"
oss_output_prefix = "${OSS_OUTPUT_PREFIX}"
date_str = "${DATE}"

files_to_upload = [
    "all_results_with_scores.csv",
    "authority_hosts.csv",
    "filtered_qna.csv",
]

print(f"上传目标: oss://{oss_config.bucket_name}/{oss_output_prefix}/{date_str}/")
print("")

for filename in files_to_upload:
    local_path = os.path.join(output_dir, filename)

    if not os.path.exists(local_path):
        print(f"⚠️  文件不存在，跳过: {local_path}")
        continue

    # OSS路径: output/results/YYYYMMDD/filename.csv
    oss_key = f"{oss_output_prefix}/{date_str}/{filename}"

    # 上传文件
    try:
        with open(local_path, "rb") as f:
            bucket.put_object(oss_key, f)

        file_size = os.path.getsize(local_path)
        print(f"✓ 上传成功: {filename} ({file_size:,} bytes)")
        print(f"  OSS路径: oss://{oss_config.bucket_name}/{oss_key}")
    except Exception as e:
        print(f"✗ 上传失败: {filename}, 错误: {e}")
        sys.exit(1)

print("")
print("所有文件上传完成！")
EOF

if [ $? -ne 0 ]; then
    echo ""
    echo "错误: OSS上传失败，请检查日志"
    exit 1
fi

echo ""
echo "========================================================================"
echo "流程执行完成！"
echo "========================================================================"
echo "本地输出文件："
echo "  1. ${OUTPUT_DIR}/all_results_with_scores.csv"
echo "  2. ${OUTPUT_DIR}/authority_hosts.csv"
echo "  3. ${OUTPUT_DIR}/filtered_qna.csv"
echo ""
echo "OSS输出路径："
echo "  oss://${OSS_BUCKET}/${OSS_OUTPUT_PREFIX}/${DATE}/"
echo ""
echo "日志目录: ./logs"
echo "========================================================================"
