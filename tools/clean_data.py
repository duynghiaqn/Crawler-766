#!/usr/bin/env python3
"""
Tool clean_data.py: Tự động dọn dẹp các tệp tin snapshot & so sánh cũ quá N ngày trong data/gia_lai và data/provinces.

Các loại file được dọn dẹp (nếu cũ quá max_days, mặc định: 3 ngày):
- comparison_*.json
- details_*.json
- agencies_*.json
- communes_*.json
- scores_*.json
- data/raw/ checkpoints & raw JSON files

Đồng thời đồng bộ và loại bỏ các mốc ngày cũ đã xóa trong index.json & index_detail.json.

Sử dụng:
  python tools/clean_data.py --clean-days 3
  python tools/clean_data.py --clean-days 3 --dry-run
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

# Ensure UTF-8 console output across all platforms
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

ROOT_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT_DIR / "data"
GIA_LAI_DIR = DATA_DIR / "gia_lai"
PROVINCES_DIR = DATA_DIR / "provinces"
RAW_DIR = DATA_DIR / "raw"

TARGET_PREFIXES = (
    "comparison_",
    "details_",
    "agencies_",
    "communes_",
    "scores_",
)


def load_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        print(f"⚠️ Warning loading JSON from {path}: {exc}", file=sys.stderr)
        return None


def write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def parse_date_from_filename(filename: str) -> datetime | None:
    match = re.search(r"_(\d{8})\.json$", filename)
    if not match:
        return None
    d_str = match.group(1)
    try:
        return datetime.strptime(d_str, "%d%m%Y")
    except ValueError:
        return None


def clean_target_directory(
    target_dir: Path,
    cutoff_date: datetime,
    dry_run: bool = False,
) -> tuple[int, set[str]]:
    """Clean files matching TARGET_PREFIXES older than cutoff_date in target_dir."""
    if not target_dir.exists():
        return 0, set()

    cutoff_date_int = int(cutoff_date.strftime("%Y%m%d"))
    deleted_count = 0
    purged_dates: set[str] = set()

    print(f"\n📂 Scanning directory: {target_dir}")
    for file_path in sorted(target_dir.glob("*.json")):
        if not file_path.is_file():
            continue
        if file_path.name in ("index.json", "index_detail.json"):
            continue

        if not file_path.name.startswith(TARGET_PREFIXES):
            continue

        file_dt = parse_date_from_filename(file_path.name)
        if not file_dt:
            continue

        file_date_int = int(file_dt.strftime("%Y%m%d"))
        if file_date_int < cutoff_date_int:
            date_str = file_dt.strftime("%d%m%Y")
            purged_dates.add(date_str)
            deleted_count += 1
            if dry_run:
                print(f"   [DRY-RUN] Would delete: {file_path.name} (Date: {file_dt.strftime('%d/%m/%Y')})")
            else:
                try:
                    file_path.unlink()
                    print(f"   🗑️ Deleted: {file_path.name} (Date: {file_dt.strftime('%d/%m/%Y')})")
                except Exception as exc:
                    print(f"   ⚠️ Could not delete {file_path.name}: {exc}", file=sys.stderr)

    return deleted_count, purged_dates


def prune_index_files(
    target_dir: Path,
    cutoff_date: datetime,
    removed_dates: set[str],
    dry_run: bool = False,
) -> int:
    """Prune dates older than cutoff_date from index.json and index_detail.json."""
    if dry_run or not target_dir.exists():
        return 0

    cutoff_date_int = int(cutoff_date.strftime("%Y%m%d"))
    pruned_files = 0

    for idx_name in ("index.json", "index_detail.json"):
        idx_path = target_dir / idx_name
        data = load_json(idx_path)
        if not data or not isinstance(data, dict):
            continue

        changed = False
        avail = data.get("availableDates", [])
        if isinstance(avail, list):
            dates_to_remove = set(removed_dates)
            for d in avail:
                try:
                    dt = datetime.strptime(d, "%d%m%Y")
                    if int(dt.strftime("%Y%m%d")) < cutoff_date_int:
                        dates_to_remove.add(d)
                except ValueError:
                    pass

            if dates_to_remove or (idx_name == "index_detail.json" and len(avail) > 2):
                new_avail = [d for d in avail if d not in dates_to_remove]
                # index_detail.json chỉ lưu tối đa 2 kỳ (kỳ hiện tại và kỳ trước đó)
                if idx_name == "index_detail.json" and len(new_avail) > 2:
                    extra_remove = set(new_avail[:-2])
                    dates_to_remove.update(extra_remove)
                    new_avail = new_avail[-2:]

                if len(new_avail) != len(avail):
                    data["availableDates"] = new_avail
                    latest_d = new_avail[-1] if new_avail else None
                    dates_before = [d for d in new_avail if d < latest_d] if latest_d else []
                    prev_d = dates_before[-1] if dates_before else None

                    data["latestDate"] = latest_d
                    data["previousDate"] = prev_d

                    # Overview history
                    ov_hist = data.get("overviewHistory", {})
                    if isinstance(ov_hist, dict):
                        for d in dates_to_remove:
                            ov_hist.pop(d, None)
                        data["latestOverview"] = ov_hist.get(latest_d) if latest_d else None
                        data["previousOverview"] = ov_hist.get(prev_d) if prev_d else None

                    # Comparisons
                    comparisons = data.get("comparisons", {})
                    if isinstance(comparisons, dict):
                        for d in dates_to_remove:
                            comparisons.pop(d, None)

                    # Gia Lai departmentsIndex
                    depts = data.get("departmentsIndex", {})
                    if isinstance(depts, dict):
                        for dept in depts.values():
                            if isinstance(dept, dict):
                                hist = dept.get("history", {})
                                if isinstance(hist, dict):
                                    for d in dates_to_remove:
                                        hist.pop(d, None)
                                    dept["latest"] = hist.get(latest_d) if latest_d else None
                                    dept["previous"] = hist.get(prev_d) if prev_d else None

                    # Gia Lai departmentsDetailIndex (index_detail.json)
                    depts_detail = data.get("departmentsDetailIndex", {})
                    if isinstance(depts_detail, dict):
                        for dept in depts_detail.values():
                            if isinstance(dept, dict):
                                hist = dept.get("history", {})
                                if isinstance(hist, dict):
                                    for d in dates_to_remove:
                                        hist.pop(d, None)
                                    dept["latest"] = hist.get(latest_d) if latest_d else None
                                    dept["previous"] = hist.get(prev_d) if prev_d else None

                    # Provinces dictionary
                    provinces = data.get("provinces", {})
                    if isinstance(provinces, dict):
                        for prov in provinces.values():
                            if isinstance(prov, dict):
                                hist = prov.get("history", {})
                                if isinstance(hist, dict):
                                    for d in dates_to_remove:
                                        hist.pop(d, None)
                                    prov["latest"] = hist.get(latest_d) if latest_d else None
                                    prov["previous"] = hist.get(prev_d) if prev_d else None

                    write_json(idx_path, data)
                    changed = True
                    pruned_files += 1
                    print(f"   🔄 Pruned old dates {dates_to_remove} from {idx_name}")

    return pruned_files


def clean_raw_directory(cutoff_date: datetime, dry_run: bool = False) -> int:
    """Clean old raw JSON files and checkpoints older than cutoff_date."""
    if not RAW_DIR.exists():
        return 0

    cutoff_date_int = int(cutoff_date.strftime("%Y%m%d"))
    deleted_count = 0

    for file_path in RAW_DIR.rglob("*.json"):
        if not file_path.is_file():
            continue
        file_dt = parse_date_from_filename(file_path.name)
        if not file_dt:
            continue
        if int(file_dt.strftime("%Y%m%d")) < cutoff_date_int:
            deleted_count += 1
            if dry_run:
                print(f"   [DRY-RUN] Would delete raw file: {file_path.name}")
            else:
                try:
                    file_path.unlink()
                    print(f"   🗑️ Deleted raw file: {file_path.name}")
                except Exception as exc:
                    print(f"   ⚠️ Could not delete {file_path.name}: {exc}", file=sys.stderr)

    return deleted_count


def clean_all(clean_days: int = 3, dry_run: bool = False) -> dict[str, int]:
    # Vietnam timezone (UTC+7)
    vn_tz = timezone(timedelta(hours=7))
    now_vn = datetime.now(vn_tz)
    cutoff_date = (now_vn - timedelta(days=clean_days)).replace(hour=0, minute=0, second=0, microsecond=0)

    print("=" * 78)
    print(f"🧹 BẮT ĐẦU DỌN DẸP DỮ LIỆU CŨ QUÁ {clean_days} NGÀY")
    print(f"🕒 Thời gian hiện tại (VN): {now_vn.strftime('%d/%m/%Y %H:%M:%S %Z')}")
    print(f"📅 Mốc cutoff xóa (< {cutoff_date.strftime('%d/%m/%Y')})")
    print(f"🔍 Chế độ Dry-Run: {'BẬT (Không xóa thực tế)' if dry_run else 'TẮT (Xóa trực tiếp)'}")
    print("=" * 78)

    # 1. Clean data/gia_lai
    gl_deleted, gl_purged = clean_target_directory(GIA_LAI_DIR, cutoff_date, dry_run=dry_run)
    prune_index_files(GIA_LAI_DIR, cutoff_date, gl_purged, dry_run=dry_run)

    # 2. Clean data/provinces
    prov_deleted, prov_purged = clean_target_directory(PROVINCES_DIR, cutoff_date, dry_run=dry_run)
    prune_index_files(PROVINCES_DIR, cutoff_date, prov_purged, dry_run=dry_run)

    # 3. Clean data/raw
    raw_deleted = clean_raw_directory(cutoff_date, dry_run=dry_run)

    total_deleted = gl_deleted + prov_deleted + raw_deleted
    print("\n" + "=" * 78)
    print("✅ HOÀN THÀNH QUÁ TRÌNH DỌN DẸP!")
    print(f"📊 Kết quả: Gia Lai: {gl_deleted} file | Tỉnh/TP: {prov_deleted} file | Raw: {raw_deleted} file")
    print(f"🗑️ Tổng cộng tệp tin đã xóa: {total_deleted}")
    print("=" * 78)

    return {
        "gia_lai": gl_deleted,
        "provinces": prov_deleted,
        "raw": raw_deleted,
        "total": total_deleted,
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Tool dọn dẹp các tệp tin snapshot & so sánh cũ quá N ngày trong data/gia_lai & data/provinces."
    )
    parser.add_argument(
        "--clean-days",
        type=int,
        default=3,
        help="Số ngày lưu trữ dữ liệu (mặc định: 3 ngày, các file có mốc ngày cũ hơn sẽ bị xóa)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Chạy thử nghiệm mô phỏng, liệt kê các file sẽ xóa mà không xóa thực tế",
    )
    args = parser.parse_args()

    results = clean_all(clean_days=args.clean_days, dry_run=args.dry_run)
    return 0


if __name__ == "__main__":
    sys.exit(main())
