#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Công cụ xóa cache (Purge) và tạo lại cache mới (Warm-up) trên jsDelivr CDN
dành cho hệ thống Crawler 766.

Tự động thực hiện 2 giai đoạn:
1. Send Purge Requests tới purge.jsdelivr.net cho các tệp index chính:
   - data/gia_lai/index.json
   - data/gia_lai/index_detail.json
   - data/provinces/index.json
   - data/provinces/index_detail.json
2. Chờ 5 giây và gửi request GET tới cdn.jsdelivr.net để kích hoạt tạo lại (warm-up) cache mới.
"""

from __future__ import annotations

import argparse
import json
import os
import ssl
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any

# Cấu hình múi giờ Việt Nam
VN_TZ = timezone(timedelta(hours=7))

ROOT_DIR = Path(__file__).resolve().parents[1]

# Danh sách mặc định 4 tệp dữ liệu cốt lõi
DEFAULT_DATA_PATHS = [
    "data/gia_lai/index.json",
    "data/gia_lai/index_detail.json",
    "data/provinces/index.json",
    "data/provinces/index_detail.json",
]


def urlopen_safe(req: urllib.request.Request, timeout: int = 15) -> Any:
    """Mở kết nối URL với cơ chế tự động vượt lỗi SSL certificate verify failed do antivirus hoặc proxy can thiệp."""
    try:
        ctx = ssl.create_default_context()
        return urllib.request.urlopen(req, timeout=timeout, context=ctx)
    except Exception as exc:
        err_str = str(exc)
        if "CERTIFICATE_VERIFY_FAILED" in err_str or "self-signed" in err_str:
            unverified_ctx = ssl._create_unverified_context()
            return urllib.request.urlopen(req, timeout=timeout, context=unverified_ctx)
        raise


def get_vn_time_str(fmt: str = "%d/%m/%Y %H:%M:%S") -> str:
    """Lấy chuỗi thời gian hiện tại theo giờ Việt Nam (UTC+7)."""
    return datetime.now(timezone.utc).astimezone(VN_TZ).strftime(fmt)


def build_urls(
    repo: str = "duynghiaqn/Crawler-766",
    branch: str = "main",
    paths: list[str] | None = None,
) -> tuple[list[str], list[str]]:
    """Tạo danh sách Purge URLs và CDN Warm-up URLs tương ứng."""
    target_paths = paths or DEFAULT_DATA_PATHS
    purge_urls = [f"https://purge.jsdelivr.net/gh/{repo}@{branch}/{p.lstrip('/')}" for p in target_paths]
    cdn_urls = [f"https://cdn.jsdelivr.net/gh/{repo}@{branch}/{p.lstrip('/')}" for p in target_paths]
    return purge_urls, cdn_urls


def send_purge_request(url: str, timeout: int = 15) -> tuple[bool, str]:
    """Gửi một request xóa cache tới jsDelivr."""
    headers = {
        "User-Agent": "Crawler766-PurgeCache/1.0",
        "Accept": "application/json, text/plain, */*",
    }
    req = urllib.request.Request(url, headers=headers, method="GET")
    try:
        with urlopen_safe(req, timeout=timeout) as resp:
            status = resp.status
            body = resp.read().decode("utf-8", errors="replace").strip()
            return status in (200, 201), body
    except urllib.error.HTTPError as exc:
        err_text = exc.read().decode("utf-8", errors="replace").strip()
        return False, f"HTTP {exc.code}: {err_text}"
    except Exception as exc:
        return False, f"Error: {exc}"


def send_warmup_request(url: str, timeout: int = 20) -> tuple[bool, int, str]:
    """Gửi một request GET tới CDN để kích hoạt jsDelivr nạp cache tệp mới."""
    headers = {
        "User-Agent": "Crawler766-CacheWarmup/1.0 (Mozilla/5.0 compatible)",
        "Accept": "application/json, */*",
        "Cache-Control": "no-cache",
    }
    req = urllib.request.Request(url, headers=headers, method="GET")
    try:
        with urlopen_safe(req, timeout=timeout) as resp:
            status = resp.status
            # Đọc một đoạn đầu để kích hoạt CDN tải về đầy đủ
            resp.read(4096)
            return status == 200, status, "OK"
    except urllib.error.HTTPError as exc:
        return False, exc.code, f"HTTP Error {exc.code}"
    except Exception as exc:
        return False, 0, str(exc)


def purge_and_warmup_jsdelivr_cache(
    repo: str = "duynghiaqn/Crawler-766",
    branch: str = "main",
    paths: list[str] | None = None,
    wait_sec: int = 5,
    verbose: bool = True,
) -> bool:
    """Thực thi toàn bộ quy trình: Gửi Purge -> Đợi -> Tạo lại Cache (Warm-up)."""
    purge_urls, cdn_urls = build_urls(repo=repo, branch=branch, paths=paths)

    sep = "=" * 78
    if verbose:
        print(f"\n{sep}")
        print(f"🚀 [jsDelivr CDN] BẮT ĐẦU XÓA CACHE & TẠO CACHE DỮ LIỆU MỚI")
        print(f"⏰ Thời gian: {get_vn_time_str()} | Branch: {branch} | Repo: {repo}")
        print(f"{sep}\n", flush=True)

    # --------------------------------------------------------------------------
    # Giai đoạn 1: Send Purge Requests to jsDelivr
    # --------------------------------------------------------------------------
    if verbose:
        print("📤 [1/2] Đang gửi yêu cầu Xóa Cache (Purge) tới jsDelivr CDN...")

    purge_all_ok = True
    for p_url in purge_urls:
        if verbose:
            print(f"  🔄 Requesting purge for: {p_url}")
        ok, res_text = send_purge_request(p_url)
        if ok:
            # Rút gọn hiển thị JSON
            try:
                parsed = json.loads(res_text)
                status_str = parsed.get("status", "ok")
                req_id = parsed.get("id", "")
                if verbose:
                    print(f"     ✅ jsDelivr response: status={status_str}, id={req_id}")
            except Exception:
                if verbose:
                    print(f"     ✅ jsDelivr response: {res_text[:120]}")
        else:
            purge_all_ok = False
            if verbose:
                print(f"     ⚠️ Thất bại khi purge: {res_text}", file=sys.stderr)

    if verbose:
        print(f"  ✅ Đã gửi xong toàn bộ {len(purge_urls)} yêu cầu purge.")

    # --------------------------------------------------------------------------
    # Nghỉ chờ jsDelivr CDN xóa sạch cache cũ ở các edge servers
    # --------------------------------------------------------------------------
    if wait_sec > 0:
        if verbose:
            print(f"\n⏳ Chờ {wait_sec} giây để jsDelivr hoàn tất xóa cache cũ trên toàn bộ CDN edge...")
        time.sleep(wait_sec)

    # --------------------------------------------------------------------------
    # Giai đoạn 2: Warm-up jsDelivr CDN Cache (Tạo lại cache dữ liệu mới từ GitHub)
    # --------------------------------------------------------------------------
    if verbose:
        print(f"\n🔥 [2/2] Kích hoạt tạo lại cache dữ liệu mới qua jsDelivr CDN...")

    warmup_all_ok = True
    for c_url in cdn_urls:
        if verbose:
            print(f"  🔥 Warm-up cache cho: {c_url}")
        ok, code, msg = send_warmup_request(c_url)
        if ok:
            if verbose:
                print(f"     ✅ HTTP {code} OK - Cache mới đã được tạo thành công.")
        else:
            warmup_all_ok = False
            if verbose:
                print(f"     ⚠️ Cảnh báo: HTTP status code trả về {code} ({msg})", file=sys.stderr)

    if verbose:
        print(f"\n{sep}")
        if purge_all_ok and warmup_all_ok:
            print(f"🎉 HOÀN TẤT PURGE VÀ TẠO LẠI CACHE JSDELIVR CDN THÀNH CÔNG!")
        else:
            print(f"⚠️ Quá trình hoàn tất với một số cảnh báo kiểm tra kết nối.")
        print(f"{sep}\n", flush=True)

    return purge_all_ok and warmup_all_ok


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Xóa cache và tạo lại cache dữ liệu mới trên jsDelivr CDN cho Crawler 766",
    )
    parser.add_argument(
        "--repo",
        type=str,
        default=os.environ.get("GITHUB_REPOSITORY", "duynghiaqn/Crawler-766"),
        help="Đường dẫn repository GitHub (mặc định: duynghiaqn/Crawler-766)",
    )
    parser.add_argument(
        "--branch",
        type=str,
        default=os.environ.get("GIT_BRANCH", "main"),
        help="Tên nhánh Git (mặc định: main)",
    )
    parser.add_argument(
        "--wait",
        type=int,
        default=5,
        help="Số giây chờ jsDelivr xóa cache trước khi warm-up (mặc định: 5s)",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Không in log chi tiết",
    )

    args = parser.parse_args()

    success = purge_and_warmup_jsdelivr_cache(
        repo=args.repo,
        branch=args.branch,
        wait_sec=args.wait,
        verbose=not args.quiet,
    )
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
