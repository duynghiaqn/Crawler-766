#!/usr/bin/env bash
# ==============================================================================
# Auto_all.sh - Automated Master Execution Script for Crawler 766
# Runs all 4 main DVCQG crawler pipelines sequentially:
#   1. Gia Lai General Scores & Indexer         (crawl_gl.py)
#   2. Gia Lai Detailed Sub-Metrics             (crawl_gl_detail.py)
#   3. All Provinces & Cities Index             (crawl_province.py)
#   4. All Provinces & Cities Detailed Sub-Metrics (crawl_province_detail.py)
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
  --push              Tự động git commit và push data/ lên repo
  --purge             Gửi Purge Requests tới jsDelivr và tạo lại cache mới
  --skip-purge        Bỏ qua bước purge và tạo lại cache jsDelivr sau khi push
  -h, --help          Hiển thị trợ giúp này

Ví dụ:
  ./tools/Auto_all.sh --time-type year --year 2026
  ./tools/Auto_all.sh --time-type month --year 2026 --period 3
  ./tools/Auto_all.sh --push
  ./tools/Auto_all.sh --push --purge
EOF
  exit 0
}

# Parse CLI flags
ARGS=()
PUSH="false"
PURGE="false"
SKIP_PURGE="false"
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
    --push)
      PUSH="true"
      shift
      ;;
    --purge)
      PURGE="true"
      shift
      ;;
    --skip-purge)
      SKIP_PURGE="true"
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
  echo "🔍 [0/4] Kiểm tra kết nối tới Cổng DVCQG (test_dvcqg_connectivity.py)..."
  python3 "${SCRIPT_DIR}/test_dvcqg_connectivity.py" --allow-fail || true
fi

# 1. Run Gia Lai General Crawler
echo ""
echo "------------------------------------------------------------------------------"
echo "📊 [1/4] Thực thi Crawler DVCQG Gia Lai (crawl_gl.py)..."
echo "------------------------------------------------------------------------------"
python3 "${SCRIPT_DIR}/crawl_gl.py" "${COMMON_ARGS[@]}"

# 2. Run Gia Lai Detailed Sub-Metrics Crawler
echo ""
echo "------------------------------------------------------------------------------"
echo "🔬 [2/4] Thực thi Crawler Chi tiết Chỉ tiêu Con Gia Lai (crawl_gl_detail.py)..."
echo "------------------------------------------------------------------------------"
python3 "${SCRIPT_DIR}/crawl_gl_detail.py" "${COMMON_ARGS[@]}"

# 3. Run All Provinces & Cities Index Crawler
echo ""
echo "------------------------------------------------------------------------------"
echo "🏛️ [3/4] Thực thi Crawler UBND Tất cả Tỉnh / Thành phố Index (crawl_province.py)..."
echo "------------------------------------------------------------------------------"
python3 "${SCRIPT_DIR}/crawl_province.py" "${COMMON_ARGS[@]}"

# 4. Run All Provinces & Cities Detailed Crawler
echo ""
echo "------------------------------------------------------------------------------"
echo "🔬 [4/4] Thực thi Crawler UBND Tất cả Tỉnh / Thành phố Chi tiết (crawl_province_detail.py)..."
echo "------------------------------------------------------------------------------"
python3 "${SCRIPT_DIR}/crawl_province_detail.py" "${COMMON_ARGS[@]}"

# 5. Auto Clean Old Data Snapshots
echo ""
echo "------------------------------------------------------------------------------"
echo "🧹 Dọn dẹp dữ liệu cũ quá ${CLEAN_DAYS} ngày (clean_data.py)..."
echo "------------------------------------------------------------------------------"
if [[ -f "${SCRIPT_DIR}/clean_data.py" ]]; then
  python3 "${SCRIPT_DIR}/clean_data.py" --clean-days "${CLEAN_DAYS}" || true
fi

# 6. Auto Git Commit and Push (if --push requested)
if [[ "${PUSH}" == "true" ]]; then
  echo ""
  echo "------------------------------------------------------------------------------"
  echo "📤 [Git] Tự động Commit và Push Dữ liệu data/ lên Git Repository..."
  echo "------------------------------------------------------------------------------"
  git add "${WORKSPACE_DIR}/data" || true
  if git diff --staged --quiet; then
    echo "ℹ️  Không có dữ liệu mới để commit."
  else
    NOW=$(TZ='Asia/Ho_Chi_Minh' date +'%d/%m/%Y %H:%M:%S' 2>/dev/null || date +'%Y-%m-%d %H:%M:%S')
    BRANCH=$(git branch --show-current 2>/dev/null || echo "main")
    git commit -m "📊 Auto Update DVCQG Data ($NOW)" || true
    for i in 1 2 3; do
      echo "🔄 Đang đẩy lên origin/${BRANCH} (Lần thử ${i}/3)..."
      if git pull --rebase --autostash -X ours origin "${BRANCH}" && git push origin "${BRANCH}"; then
        echo "✅ Dữ liệu đã được push lên GitHub thành công!"
        break
      fi
      sleep 3
    done
  fi
fi

# 7. Send Purge Requests to jsDelivr & Recreate/Warm-up Cache
# Tự động kích hoạt khi có đồng bộ git (--push) hoặc được chỉ định rõ (--purge), trừ khi có --skip-purge
if [[ ("${PUSH}" == "true" || "${PURGE}" == "true") && "${SKIP_PURGE}" != "true" ]]; then
  echo ""
  echo "------------------------------------------------------------------------------"
  echo "⚡ [7/7] Send Purge Requests to jsDelivr & Tạo lại cache dữ liệu mới..."
  echo "------------------------------------------------------------------------------"
  if [[ -f "${SCRIPT_DIR}/purge_cache.py" ]]; then
    python3 "${SCRIPT_DIR}/purge_cache.py" || true
  else
    PURGE_URLS=(
      "https://purge.jsdelivr.net/gh/duynghiaqn/Crawler-766@main/data/gia_lai/index.json"
      "https://purge.jsdelivr.net/gh/duynghiaqn/Crawler-766@main/data/gia_lai/index_detail.json"
      "https://purge.jsdelivr.net/gh/duynghiaqn/Crawler-766@main/data/provinces/index.json"
      "https://purge.jsdelivr.net/gh/duynghiaqn/Crawler-766@main/data/provinces/index_detail.json"
    )
    echo "🚀 Purging jsDelivr CDN cache at $(TZ='Asia/Ho_Chi_Minh' date +'%Y-%m-%d %H:%M:%S %Z')..."
    for url in "${PURGE_URLS[@]}"; do
      echo "  🔄 Requesting purge for: $url"
      response=$(curl -s -X GET "$url")
      echo "  📩 Response: $response"
    done

    echo "⏳ Chờ 5 giây để jsDelivr hoàn tất xóa cache cũ..."
    sleep 5

    CDN_URLS=(
      "https://cdn.jsdelivr.net/gh/duynghiaqn/Crawler-766@main/data/gia_lai/index.json"
      "https://cdn.jsdelivr.net/gh/duynghiaqn/Crawler-766@main/data/gia_lai/index_detail.json"
      "https://cdn.jsdelivr.net/gh/duynghiaqn/Crawler-766@main/data/provinces/index.json"
      "https://cdn.jsdelivr.net/gh/duynghiaqn/Crawler-766@main/data/provinces/index_detail.json"
    )
    echo "🔥 Đang kích hoạt tạo lại cache dữ liệu mới qua jsDelivr CDN..."
    for url in "${CDN_URLS[@]}"; do
      echo "  🔥 Warm-up cache cho: $url"
      http_code=$(curl -s -L -o /dev/null -w "%{http_code}" "$url")
      echo "  📩 HTTP Status Code: $http_code"
      if [ "$http_code" -eq 200 ]; then
        echo "  ✅ Cache mới đã được tạo thành công."
      else
        echo "  ⚠️ Cảnh báo: HTTP status code trả về $http_code"
      fi
    done
  fi
fi

END_TIME=$(date +%s)
ELAPSED=$((END_TIME - START_TIME))

echo ""
echo "=============================================================================="
echo "✅ CHẠY THÀNH CÔNG TOÀN BỘ CÁC CRAWLER PIPELINES!"
echo "⏱️  Tổng thời gian thực thi: ${ELAPSED} giây"
echo "=============================================================================="
