#!/usr/bin/env bash
# ==============================================================================
# Auto_all.sh - Automated Master Execution Script for Crawler 766
# Runs all 3 main DVCQG crawler pipelines sequentially:
#   1. Gia Lai General Scores & Indexer  (crawl_gl.py)
#   2. Gia Lai Detailed Sub-Metrics      (crawl_gl_detail.py)
#   3. All Provinces & Cities Nationwide (crawl_province.py)
# ==============================================================================

set -eo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Default parameters
TIME_TYPE="year"
YEAR=$(date +%Y)
PERIOD=""
CLEAN_DAYS="3"
SKIP_TEST="false"

# Help message
show_help() {
  cat << EOF
🚀 Master Auto Crawler Script (Auto_all.sh) - Crawler 766

Sử dụng:
  ./tools/Auto_all.sh [OPTIONS]

Tùy chọn:
  --time-type TYPE    Loại thời gian: year (mặc định), month, quarter
  --year YEAR         Năm trích xuất (Mặc định: năm hiện tại)
  --period PERIOD     Kỳ trích xuất (Tháng 1-12 hoặc Quý 1-4)
  --clean-days DAYS   Số ngày tự động dọn dẹp snapshot cũ (Mặc định: 3)
  --skip-test         Bỏ qua bước kiểm tra kết nối DVCQG
  -h, --help          Hiển thị trợ giúp này

Ví dụ:
  ./tools/Auto_all.sh --time-type year --year 2026
  ./tools/Auto_all.sh --time-type month --year 2026 --period 3
EOF
  exit 0
}

# Parse CLI flags
ARGS=()
while [[ $# -gt 0 ]]; do
  case $1 in
    --time-type)
      TIME_TYPE="$2"
      shift 2
      ;;
    --year)
      YEAR="$2"
      shift 2
      ;;
    --period)
      PERIOD="$2"
      shift 2
      ;;
    --clean-days)
      CLEAN_DAYS="$2"
      shift 2
      ;;
    --skip-test)
      SKIP_TEST="true"
      shift
      ;;
    -h|--help)
      show_help
      ;;
    *)
      ARGS+=("$1")
      shift
      ;;
  esac
done

echo "=============================================================================="
echo "🤖 KHỞI ĐỘNG HỆ THỐNG MASTER AUTO CRAWLER 766"
echo "📅 Ngày thực thi: $(date '+%Y-%m-%d %H:%M:%S')"
echo "⚙️  Tham số: time_type=${TIME_TYPE} | year=${YEAR} | period=${PERIOD:-N/A} | clean_days=${CLEAN_DAYS}"
echo "=============================================================================="

# Build common arguments array
COMMON_ARGS=("--time-type" "${TIME_TYPE}" "--year" "${YEAR}" "--clean-days" "${CLEAN_DAYS}")
if [[ -n "${PERIOD}" ]]; then
  COMMON_ARGS+=("--period" "${PERIOD}")
fi
if [[ ${#ARGS[@]} -gt 0 ]]; then
  COMMON_ARGS+=("${ARGS[@]}")
fi

START_TIME=$(date +%s)

# 0. Connectivity test (optional)
if [[ "${SKIP_TEST}" != "true" && -f "${SCRIPT_DIR}/test_dvcqg_connectivity.py" ]]; then
  echo ""
  echo "🔍 [0/3] Kiểm tra kết nối tới Cổng DVCQG (test_dvcqg_connectivity.py)..."
  python3 "${SCRIPT_DIR}/test_dvcqg_connectivity.py" --allow-fail || true
fi

# 1. Run Gia Lai General Crawler
echo ""
echo "------------------------------------------------------------------------------"
echo "📊 [1/3] Thực thi Crawler DVCQG Gia Lai (crawl_gl.py)..."
echo "------------------------------------------------------------------------------"
python3 "${SCRIPT_DIR}/crawl_gl.py" "${COMMON_ARGS[@]}"

# 2. Run Gia Lai Detailed Sub-Metrics Crawler
echo ""
echo "------------------------------------------------------------------------------"
echo "🔬 [2/3] Thực thi Crawler Chi tiết Chỉ tiêu Con Gia Lai (crawl_gl_detail.py)..."
echo "------------------------------------------------------------------------------"
python3 "${SCRIPT_DIR}/crawl_gl_detail.py" "${COMMON_ARGS[@]}"

# 3. Run All Provinces & Cities Crawler
echo ""
echo "------------------------------------------------------------------------------"
echo "🏛️ [3/3] Thực thi Crawler UBND Tất cả Tỉnh / Thành phố (crawl_province.py)..."
echo "------------------------------------------------------------------------------"
python3 "${SCRIPT_DIR}/crawl_province.py" "${COMMON_ARGS[@]}"

END_TIME=$(date +%s)
ELAPSED=$((END_TIME - START_TIME))

echo ""
echo "=============================================================================="
echo "✅ CHẠY THÀNH CÔNG TOÀN BỘ CÁC CRAWLER PIPELINES!"
echo "⏱️  Tổng thời gian thực thi: ${ELAPSED} giây"
echo "=============================================================================="
