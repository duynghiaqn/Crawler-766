#!/usr/bin/env python3
"""
Tool crawl_province_detail.py: Trích xuất 6 nhóm chỉ số thành phần & sub-metrics chi tiết, duy trì CSDL Master Index Detail (index_detail.json) cho tất cả UBND Tỉnh / Thành phố từ DVCQG.

Endpoint API công khai DVCQG:
1. API Dữ liệu tổng hợp : https://dichvucong.gov.vn/api/v1/reporting/evaluation/service-results
2. API Công khai minh bạch : https://dichvucong.gov.vn/api/v1/reporting/evaluation/transparency
3. API Tiến độ giải quyết  : https://dichvucong.gov.vn/api/v1/reporting/evaluation/dvc-progress-tree
4. API Dịch vụ công trực tuyến : https://dichvucong.gov.vn/api/v1/reporting/evaluation/provide-online-tree
5. API Thanh toán trực tuyến   : https://dichvucong.gov.vn/api/v1/reporting/evaluation/formality-online-payment-tree
6. API Số hóa hồ sơ       : https://dichvucong.gov.vn/api/v1/reporting/evaluation/dossier-digitized
7. API Mức độ hài lòng    : https://dichvucong.gov.vn/api/v1/reporting/evaluation/handling-satisfaction

Tính năng chính:
1. Trích xuất chi tiết 6 nhóm chỉ tiêu thành phần (CKMB, TDGQ, ONLINE, TTTT, MDSH, MDHL) & sub-metrics của tất cả UBND Tỉnh/Thành phố toàn quốc.
2. Lưu file CSDL Master Index Detail (index_detail.json) và file chi tiết snapshot theo mốc ngày DDMMYYYY trong data/provinces/:
   - index_detail.json               : API Detailed Index Database chứa toàn bộ số liệu chi tiết các chỉ số thành phần (componentIndicators).
   - details_Provinces_DDMMYYYY.json  : Dữ liệu chi tiết 6 nhóm chỉ tiêu & sub-metrics các tỉnh/thành phố theo ngày.
3. Engine So sánh Điểm & Thứ hạng Kỳ Liền (Consecutive Period Comparison): Tự động so sánh chênh lệch điểm số, thứ hạng và 6 chỉ số thành phần giữa mốc ngày hiện tại với ngày chạy trước đó.
4. Cơ chế chống WAF/Firewall: User-Agent Rotation Pool, Header ngẫu nhiên & Randomized Sleep Jitter (0.5s - 1.5s).
5. Khôi phục điểm ngắt lỗi (Checkpoint Resumption) tại data/raw/<year>/provinces/checkpoints/.
6. Engine Tự động dọn dẹp Snapshot & Index cũ (> N ngày, mặc định: 3 ngày / 72 giờ).

Sử dụng:
  python tools/crawl_province_detail.py --time-type year --year 2026
  python tools/crawl_province_detail.py --time-type month --year 2026 --period 3
  python tools/crawl_province_detail.py --clean-days 3
"""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
import gzip
import http.cookiejar
import json
from pathlib import Path
import random
import re
import socket
import ssl
import sys
import threading
import time
from typing import Any
import urllib.error
import urllib.request
import zlib

# Ensure UTF-8 encoding for console output on Windows / non-UTF-8 terminals
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

# Set sensible default socket timeout to prevent OS-level indefinite TCP hanging
socket.setdefaulttimeout(120)

# Global Session, CookieJar and SSL context for connection reuse & anti-WAF
COOKIE_JAR = http.cookiejar.CookieJar()
SSL_CONTEXT = ssl.create_default_context()
OPENER = urllib.request.build_opener(
    urllib.request.HTTPCookieProcessor(COOKIE_JAR),
    urllib.request.HTTPSHandler(context=SSL_CONTEXT),
)
_SESSION_WARMED_UP = False
_SESSION_LOCK = threading.Lock()

# Default Configuration Constants
ENDPOINT_SERVICE_RESULTS = "https://dichvucong.gov.vn/api/v1/reporting/evaluation/service-results"
ENDPOINT_TRANSPARENCY = "https://dichvucong.gov.vn/api/v1/reporting/evaluation/transparency"
ENDPOINT_PROGRESS = "https://dichvucong.gov.vn/api/v1/reporting/evaluation/dvc-progress-tree"
ENDPOINT_ONLINE = "https://dichvucong.gov.vn/api/v1/reporting/evaluation/provide-online-tree"
ENDPOINT_PAYMENT = "https://dichvucong.gov.vn/api/v1/reporting/evaluation/formality-online-payment-tree"
ENDPOINT_DIGITIZED = "https://dichvucong.gov.vn/api/v1/reporting/evaluation/dossier-digitized"
ENDPOINT_HANDLING_SATISFACTION = "https://dichvucong.gov.vn/api/v1/reporting/evaluation/handling-satisfaction"

ROOT_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT_DIR / "data"
DEFAULT_OUTPUT_DIR = DATA_DIR / "provinces"
DEFAULT_RAW_DIR = DATA_DIR / "raw"

# User-Agent Pool for Evasion / Anti-Blocking (Standard Desktop Chrome/Chromium)
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
]

try:
    from tqdm import tqdm
    HAS_TQDM = True
except ImportError:
    HAS_TQDM = False


class CrawlerProgressBar:
    """Flexible thread-safe progress bar supporting tqdm with fallback to clean standard console output."""

    def __init__(self, total: int, desc: str = "Crawling All Provinces Detail", unit: str = "step"):
        self.total = total
        self.desc = desc
        self.unit = unit
        self.current = 0
        self.start_time = time.time()
        self._lock = threading.Lock()
        if HAS_TQDM:
            self.pbar = tqdm(
                total=total,
                desc=desc,
                unit=unit,
                leave=True,
                bar_format="{l_bar}{bar:25}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}{postfix}]",
            )
        else:
            self.pbar = None
            self._render_fallback()

    def update(self, n: int = 1, status: str = ""):
        with self._lock:
            self.current += n
            if self.pbar:
                if status:
                    self.pbar.set_postfix_str(status)
                self.pbar.update(n)
            else:
                self._render_fallback(status)

    def set_postfix_str(self, status: str):
        with self._lock:
            if self.pbar:
                self.pbar.set_postfix_str(status)
            else:
                self._render_fallback(status)

    def _render_fallback(self, status: str = ""):
        elapsed = time.time() - self.start_time
        pct = (self.current / self.total * 100) if self.total > 0 else 0
        filled_len = int(25 * self.current // self.total) if self.total > 0 else 0
        bar = "█" * filled_len + "░" * (25 - filled_len)
        if self.current > 0 and self.current < self.total:
            eta_sec = (elapsed / self.current) * (self.total - self.current)
            eta_str = f" | ETA: {int(eta_sec // 60):02d}:{int(eta_sec % 60):02d}"
        else:
            eta_str = f" | Elapsed: {int(elapsed // 60):02d}:{int(elapsed % 60):02d}"
        status_part = f" [{status}]" if status else ""
        sys.stdout.write(f"\r⏳ {self.desc}: [{bar}] {self.current}/{self.total} ({pct:.1f}%){eta_str}{status_part}")
        sys.stdout.flush()
        if self.current >= self.total:
            sys.stdout.write("\n")

    def close(self):
        with self._lock:
            if self.pbar:
                self.pbar.close()
            elif self.current < self.total:
                sys.stdout.write("\n")


def utc_now() -> str:
    """Return ISO 8601 UTC timestamp."""
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def write_json(path: Path, data: Any) -> None:
    """Write data to JSON file with UTF-8 encoding and pretty indentation."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def load_json(path: Path) -> dict[str, Any] | None:
    """Load JSON file if exists, otherwise return None."""
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        print(f"⚠️ Warning loading JSON from {path}: {exc}", file=sys.stderr)
        return None


def decode_response_content(response: Any) -> str:
    """Read response bytes and decompress gzip/deflate if needed, returning decoded utf-8 text."""
    raw_bytes = response.read()
    content_encoding = (response.headers.get("Content-Encoding") or "").lower()
    if "gzip" in content_encoding or raw_bytes.startswith(b"\x1f\x8b"):
        try:
            return gzip.decompress(raw_bytes).decode("utf-8", errors="replace")
        except Exception:
            pass
    elif "deflate" in content_encoding:
        try:
            return zlib.decompress(raw_bytes).decode("utf-8", errors="replace")
        except Exception:
            pass
    return raw_bytes.decode("utf-8", errors="replace")


def get_random_headers() -> dict[str, str]:
    """Generate randomized stealth headers with Keep-Alive & Gzip to prevent WAF / bot detection."""
    ua = random.choice(USER_AGENTS)
    ver_match = re.search(r"Chrome/(\d+)", ua)
    ver = ver_match.group(1) if ver_match else "125"
    if "Windows" in ua:
        platform = '"Windows"'
    elif "Macintosh" in ua:
        platform = '"macOS"'
    else:
        platform = '"Linux"'

    headers = {
        "Accept": "application/json, text/plain, */*",
        "Accept-Encoding": "gzip, deflate",
        "Connection": "keep-alive",
        "Content-Type": "application/json",
        "User-Agent": ua,
        "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
        "Referer": "https://dichvucong.gov.vn/danh-gia-chat-luong-phuc-vu",
        "Origin": "https://dichvucong.gov.vn",
        "Sec-Ch-Ua": f'"Chromium";v="{ver}", "Google Chrome";v="{ver}", "Not-A.Brand";v="99"',
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": platform,
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
    }
    return headers


def warmup_session(timeout: int = 15, force: bool = False) -> bool:
    """Warmup cookies and TLS session by visiting public evaluation page."""
    global _SESSION_WARMED_UP
    with _SESSION_LOCK:
        if _SESSION_WARMED_UP and not force:
            return True
        try:
            headers = get_random_headers()
            req = urllib.request.Request(
                "https://dichvucong.gov.vn/danh-gia-chat-luong-phuc-vu",
                headers=headers,
                method="GET",
            )
            with OPENER.open(req, timeout=timeout) as resp:
                resp.read(512)
            _SESSION_WARMED_UP = True
            return True
        except Exception:
            return False


def province_short_name(name: str) -> str:
    """Normalize full department name to concise province short name."""
    val = " ".join((name or "").split()).strip()
    val = re.sub(r"^UBND\s+", "", val, flags=re.I)
    val = re.sub(r"^tỉnh\s+", "", val, flags=re.I)
    val = re.sub(r"^thành phố\s+", "", val, flags=re.I)
    return val.strip()


def fetch_dvc_endpoint(
    url: str,
    time_type: str,
    year: int,
    period: int | None = None,
    department_type: str | None = "ADMINISTRATIVE_UNIT",
    page_size: int = 100,
    current_page: int = 1,
    timeout: int = 45,
    max_retries: int = 6,
    delay_min: float = 1.0,
    delay_max: float = 2.5,
) -> dict[str, Any]:
    """Fetch DVCQG API endpoint with session cookie reuse, adaptive staged timeout, gzip decompression, and smart backoff retries."""
    global _SESSION_WARMED_UP
    payload: dict[str, Any] = {
        "timeType": time_type,
        "year": year,
        "pageSize": page_size,
        "currentPage": current_page,
    }
    if department_type:
        payload["departmentType"] = department_type

    if time_type == "quarter":
        if period is None or not (1 <= period <= 4):
            raise ValueError("Quarter period must be between 1 and 4")
        payload["quarter"] = period
    elif time_type == "month":
        if period is None or not (1 <= period <= 12):
            raise ValueError("Month period must be between 1 and 12")
        payload["month"] = period

    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")

    # Anti-blocking: Add randomized jitter delay
    sleep_sec = random.uniform(delay_min, delay_max) + random.uniform(0.05, 0.25)
    time.sleep(sleep_sec)

    # Adaptive Staged Timeout: attempt đầu nhanh gọn để tránh treo lâu, sau đó tăng dần
    base_timeout = min(timeout, 35)
    step_timeout = max(5, int((timeout - base_timeout) / max(1, max_retries - 1)))

    for attempt in range(1, max_retries + 1):
        headers = get_random_headers()
        request = urllib.request.Request(url, data=body, method="POST", headers=headers)
        current_timeout = min(timeout, base_timeout + (attempt - 1) * step_timeout)
        try:
            # LUÔN LUÔN dùng OPENER để giữ Cookie session TLS & Keep-Alive của DVCQG
            with OPENER.open(request, timeout=current_timeout) as response:
                status = response.status
                raw_text = decode_response_content(response)
                parsed = json.loads(raw_text)
                if status not in (200, 201):
                    raise RuntimeError(f"API returned HTTP status {status}")
                return parsed
        except urllib.error.HTTPError as exc:
            err_body = decode_response_content(exc)
            if exc.code in (429, 403, 500, 502, 503, 504) and attempt < max_retries:
                retry_wait = (2.5 * (1.35 ** (attempt - 1))) + random.uniform(1.0, 3.0)
                print(f"\n⚠️ [Thử lại {attempt}/{max_retries}] HTTP {exc.code} từ DVCQG. Đợi {retry_wait:.1f}s...", file=sys.stderr)
                if exc.code == 403:
                    warmup_session(timeout=min(20, timeout), force=True)
                time.sleep(retry_wait)
                continue
            raise RuntimeError(f"HTTP Error {exc.code}: {err_body[:500]}") from exc
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            reason = getattr(exc, "reason", str(exc))
            if attempt < max_retries:
                retry_wait = (2.5 * (1.35 ** (attempt - 1))) + random.uniform(1.0, 3.0)
                print(f"\n⚠️ [Thử lại {attempt}/{max_retries}] Lỗi kết nối DVCQG ({reason}). Tự động thử lại sau {retry_wait:.1f}s...", file=sys.stderr)
                warmup_session(timeout=min(20, timeout), force=True)
                time.sleep(retry_wait)
                continue
            raise RuntimeError(f"Network error connecting to DVCQG: {reason}") from exc

    raise RuntimeError(f"Failed to fetch DVCQG endpoint after {max_retries} attempts")


def fetch_service_results_via_playwright(
    time_type: str,
    year: int,
    period: int | None = None,
    timeout_sec: int = 60,
) -> dict[str, Any] | None:
    """Fallback fetch national service-results using real Chromium browser via Playwright."""
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        return None

    payload: dict[str, Any] = {
        "timeType": time_type,
        "year": year,
        "departmentType": "ADMINISTRATIVE_UNIT",
        "pageSize": 100,
        "currentPage": 1,
    }
    if time_type == "quarter" and period:
        payload["quarter"] = period
    elif time_type == "month" and period:
        payload["month"] = period

    print("\n🌐 [Playwright Fallback] Khởi tạo Playwright Chromium để bypass WAF & lấy dữ liệu National Service Results...", file=sys.stderr)
    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True)
            context = browser.new_context(locale="vi-VN")
            page = context.new_page()
            page.goto("https://dichvucong.gov.vn/danh-gia-chat-luong-phuc-vu", timeout=45000, wait_until="commit")
            time.sleep(2)
            resp_data = page.evaluate(
                """async ([url, payload]) => {
                    const res = await fetch(url, {
                        method: 'POST',
                        headers: {
                            'Content-Type': 'application/json',
                            'Accept': 'application/json, text/plain, */*'
                        },
                        body: JSON.stringify(payload)
                    });
                    if (!res.ok) throw new Error('HTTP ' + res.status);
                    return await res.json();
                }""",
                [ENDPOINT_SERVICE_RESULTS, payload],
            )
            browser.close()
            if resp_data and isinstance(resp_data, dict) and "data" in resp_data:
                print("✅ [Playwright Fallback] Lấy dữ liệu thành công qua Chromium!", file=sys.stderr)
                return resp_data
    except Exception as exc:
        print(f"⚠️ [Playwright Fallback Error]: {exc}", file=sys.stderr)
    return None


def fetch_all_provinces_service_results(
    time_type: str,
    year: int,
    period: int | None = None,
    checkpoint_file: Path | None = None,
    raw_file: Path | None = None,
    timeout: int = 45,
    max_retries: int = 6,
    delay_min: float = 1.0,
    delay_max: float = 2.5,
) -> dict[str, Any]:
    """Fetch national service-results overview containing all provinces with checkpoint resume & multi-strategy fallback."""
    if checkpoint_file and checkpoint_file.exists():
        cached = load_json(checkpoint_file)
        if isinstance(cached, dict) and cached.get("data", {}).get("evaluation"):
            print("  ℹ️ Khôi phục Dữ liệu Tổng quan Tỉnh/TP từ Checkpoint đĩa.")
            return cached
    if raw_file and raw_file.exists():
        cached = load_json(raw_file)
        if isinstance(cached, dict) and cached.get("data", {}).get("evaluation"):
            print("  ℹ️ Khôi phục Dữ liệu Tổng quan Tỉnh/TP từ file Raw đĩa.")
            return cached

    # Strategy 1: Direct API
    try:
        data = fetch_dvc_endpoint(
            ENDPOINT_SERVICE_RESULTS,
            time_type,
            year,
            period,
            department_type="ADMINISTRATIVE_UNIT",
            page_size=100,
            current_page=1,
            timeout=timeout,
            max_retries=max_retries,
            delay_min=delay_min,
            delay_max=delay_max,
        )
        eval_items = data.get("data", {}).get("evaluation", [])
        if isinstance(eval_items, list) and len(eval_items) > 0:
            if checkpoint_file:
                write_json(checkpoint_file, data)
            return data
    except Exception as exc:
        print(f"\n⚠️ [Chiến lược 1] Truy vấn ADMINISTRATIVE_UNIT chưa thành công: {exc}", file=sys.stderr)

    # Strategy 2: Global National Overview
    print("\n🔄 [Chiến lược 2] Chuyển đổi sang truy vấn tổng hợp quốc gia (Global National Overview)...", file=sys.stderr)
    try:
        data = fetch_dvc_endpoint(
            ENDPOINT_SERVICE_RESULTS,
            time_type,
            year,
            period,
            department_type=None,
            page_size=200,
            current_page=1,
            timeout=timeout,
            max_retries=max_retries,
            delay_min=delay_min,
            delay_max=delay_max,
        )
        eval_list = data.get("data", {}).get("evaluation", [])
        if isinstance(eval_list, list) and len(eval_list) > 0:
            prov_items = [
                row for row in eval_list
                if isinstance(row, dict) and (
                    str(row.get("departmentCode", "")).startswith("H")
                    or str(row.get("departmentName", "")).startswith("UBND")
                )
            ]
            if len(prov_items) >= 20:
                print(f"✅ [Chiến lược 2] Lấy thành công {len(prov_items)} Tỉnh/Thành phố từ dữ liệu tổng hợp!", file=sys.stderr)
                data["data"]["evaluation"] = prov_items
                if checkpoint_file:
                    write_json(checkpoint_file, data)
                return data
    except Exception as exc:
        print(f"⚠️ [Chiến lược 2] Truy vấn tổng hợp quốc gia không thành công: {exc}", file=sys.stderr)

    # Strategy 3: Playwright Chromium browser fallback
    print("\n🌐 [Chiến lược 3] Khởi chạy Playwright Chromium để render & bypass WAF...", file=sys.stderr)
    pw_result = fetch_service_results_via_playwright(time_type, year, period, timeout_sec=timeout)
    if pw_result and isinstance(pw_result.get("data", {}).get("evaluation"), list) and len(pw_result["data"]["evaluation"]) > 0:
        if checkpoint_file:
            write_json(checkpoint_file, pw_result)
        return pw_result

    raise RuntimeError("Tất cả các cơ chế kết nối DVCQG (Direct API, Global Strategy, Playwright) đều không thành công.")


def fetch_group_via_playwright(
    group_code: str,
    url: str,
    time_type: str,
    year: int,
    period: int | None = None,
    timeout_sec: int = 60,
) -> dict[str, Any] | None:
    """Fallback fetch single component group via Playwright Chromium when direct API fails."""
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        return None

    payload: dict[str, Any] = {
        "timeType": time_type,
        "year": year,
        "departmentType": "ADMINISTRATIVE_UNIT",
        "pageSize": 100,
        "currentPage": 1,
    }
    if time_type == "quarter" and period:
        payload["quarter"] = period
    elif time_type == "month" and period:
        payload["month"] = period

    endpoint_name = url.rstrip("/").split("/")[-1]
    print(f"\n🌐 [Playwright Fallback] Khởi tạo Chromium để lấy dữ liệu nhóm {group_code} ({endpoint_name})...", file=sys.stderr)
    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True)
            context = browser.new_context(locale="vi-VN")
            page = context.new_page()
            page.goto("https://dichvucong.gov.vn/danh-gia-chat-luong-phuc-vu", timeout=45000, wait_until="commit")
            time.sleep(1.5)
            resp_data = page.evaluate(
                """async ([targetUrl, payload]) => {
                    const res = await fetch(targetUrl, {
                        method: 'POST',
                        headers: {
                            'Content-Type': 'application/json',
                            'Accept': 'application/json, text/plain, */*'
                        },
                        body: JSON.stringify(payload)
                    });
                    if (!res.ok) throw new Error('HTTP ' + res.status);
                    return await res.json();
                }""",
                [url, payload],
            )
            browser.close()
            if resp_data and isinstance(resp_data, dict) and "data" in resp_data:
                print(f"✅ [Playwright Fallback] Lấy dữ liệu thành công cho nhóm {group_code} qua Chromium!", file=sys.stderr)
                return resp_data
    except Exception as exc:
        print(f"⚠️ [Playwright Fallback Error - {group_code}]: {exc}", file=sys.stderr)
    return None


def _fetch_group_map_for_provinces(
    group_code: str,
    url: str,
    data_key: str,
    time_type: str,
    year: int,
    period: int | None,
    timeout: int,
    max_retries: int,
    delay_min: float = 1.0,
    delay_max: float = 2.0,
    pbar: CrawlerProgressBar | None = None,
) -> tuple[str, dict[str, float], dict[str, dict[str, Any]]]:
    """Worker function to fetch a single component indicator endpoint across all provinces with Playwright fallback."""
    result_map: dict[str, float] = {}
    item_map: dict[str, dict[str, Any]] = {}
    resp = None
    try:
        if pbar:
            pbar.set_postfix_str(f"Fetching {group_code}...")
        resp = fetch_dvc_endpoint(
            url,
            time_type,
            year,
            period,
            department_type="ADMINISTRATIVE_UNIT",
            page_size=100,
            current_page=1,
            timeout=timeout,
            max_retries=max_retries,
            delay_min=delay_min,
            delay_max=delay_max,
        )
    except Exception as exc:
        print(f"\n⚠️ Direct API fetch failed for {group_code}: {exc}. Kích hoạt Playwright Fallback...", file=sys.stderr)
        resp = fetch_group_via_playwright(group_code, url, time_type, year, period, timeout_sec=timeout)

    if resp and isinstance(resp, dict):
        data = resp.get("data", {}) if isinstance(resp, dict) else {}
        items = data.get(data_key, [])
        if not isinstance(items, list):
            items = data.get("evaluation", [])
        if isinstance(items, list):
            for item in items:
                if isinstance(item, dict):
                    did = item.get("departmentId") or item.get("departmentCode")
                    score_val = item.get("score") if item.get("score") is not None else item.get("totalScore", 0)
                    if did:
                        result_map[did] = round(float(score_val), 2)
                        item_map[did] = item
                        code = item.get("departmentCode")
                        if code:
                            result_map[code] = round(float(score_val), 2)
                            item_map[code] = item
        if pbar:
            pbar.update(1, status=f"{group_code} OK")
    else:
        print(f"\n⚠️ Không thể lấy dữ liệu cho nhóm chỉ tiêu {group_code}.", file=sys.stderr)
        if pbar:
            pbar.update(1, status=f"{group_code} Warn")

    return group_code, result_map, item_map


def fetch_provinces_component_groups_maps(
    time_type: str,
    year: int,
    period: int | None = None,
    checkpoint_file: Path | None = None,
    timeout: int = 45,
    max_retries: int = 6,
    concurrency: int = 3,
    delay_min: float = 1.0,
    delay_max: float = 2.0,
    pbar: CrawlerProgressBar | None = None,
) -> tuple[dict[str, dict[str, float]], dict[str, dict[str, dict[str, Any]]]]:
    """Fetch all 6 component criteria group endpoints concurrently for all provinces, returning score maps and item detail maps."""
    maps: dict[str, dict[str, float]] = {
        "CKMB": {},
        "TDGQ": {},
        "ONLINE": {},
        "TTTT": {},
        "MDSH": {},
        "MDHL": {},
    }
    item_maps: dict[str, dict[str, dict[str, Any]]] = {
        "CKMB": {},
        "TDGQ": {},
        "ONLINE": {},
        "TTTT": {},
        "MDSH": {},
        "MDHL": {},
    }

    if checkpoint_file and checkpoint_file.exists():
        cached = load_json(checkpoint_file)
        if isinstance(cached, dict) and "maps" in cached:
            c_maps = cached.get("maps", {})
            c_items = cached.get("item_maps", {})
            for k in maps:
                if k in c_maps and isinstance(c_maps[k], dict) and len(c_maps[k]) > 0:
                    maps[k] = c_maps[k]
                if k in c_items and isinstance(c_items[k], dict) and len(c_items[k]) > 0:
                    item_maps[k] = c_items[k]

    tasks = [
        ("CKMB", ENDPOINT_TRANSPARENCY, "evaluation"),
        ("TDGQ", ENDPOINT_PROGRESS, "children"),
        ("ONLINE", ENDPOINT_ONLINE, "children"),
        ("TTTT", ENDPOINT_PAYMENT, "children"),
        ("MDSH", ENDPOINT_DIGITIZED, "evaluation"),
        ("MDHL", ENDPOINT_HANDLING_SATISFACTION, "evaluation"),
    ]

    pending_tasks = [t for t in tasks if not maps[t[0]]]
    completed_count = len(tasks) - len(pending_tasks)

    if completed_count > 0:
        if pbar:
            pbar.update(completed_count, status=f"Checkpoint {completed_count}/6 OK")
        print(f"  ℹ️ Khôi phục {completed_count}/6 nhóm chỉ tiêu từ mốc checkpoint đĩa.")

    if not pending_tasks:
        return maps, item_maps

    lock = threading.Lock()

    def _worker(group_code: str, url: str, data_key: str) -> tuple[str, dict[str, float], dict[str, dict[str, Any]]]:
        time.sleep(random.uniform(0.1, 0.5))
        g_code, g_map, g_items = _fetch_group_map_for_provinces(
            group_code, url, data_key, time_type, year, period, timeout, max_retries, delay_min, delay_max, pbar
        )
        with lock:
            if g_map:
                maps[g_code] = g_map
                item_maps[g_code] = g_items
                if checkpoint_file:
                    write_json(checkpoint_file, {"maps": maps, "item_maps": item_maps, "updatedAt": utc_now()})
        return g_code, g_map, g_items

    if concurrency <= 1 or len(pending_tasks) == 1:
        for group_code, url, data_key in pending_tasks:
            _worker(group_code, url, data_key)
    else:
        max_workers = min(concurrency, len(pending_tasks))
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = []
            for g_code, url, d_key in pending_tasks:
                futures.append(executor.submit(_worker, g_code, url, d_key))
                time.sleep(random.uniform(0.6, 1.2))  # Staggered launch to prevent bursting DVCQG WAF
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as exc:
                    print(f"\n⚠️ Thread partition error: {exc}", file=sys.stderr)

    return maps, item_maps


def extract_province_score_data(
    raw_response: dict[str, Any],
    time_type: str,
    year: int,
    period: int | None = None,
    component_group_maps: dict[str, dict[str, float]] | None = None,
    component_group_item_maps: dict[str, dict[str, dict[str, Any]]] | None = None,
    run_date_str: str | None = None,
) -> dict[str, Any]:
    """Extract, rank, and normalize evaluation scores & 6 detailed component indicators for all provinces."""
    data = raw_response.get("data")
    if not isinstance(data, dict):
        raise ValueError("Invalid response schema: missing 'data' dictionary")

    overview = data.get("overview") or {}
    evaluation_raw = data.get("evaluation") or []
    if not isinstance(evaluation_raw, list):
        evaluation_raw = []

    cg_maps = component_group_maps or {}
    ci_maps = component_group_item_maps or {}

    provinces: list[dict[str, Any]] = []

    for item in evaluation_raw:
        if not isinstance(item, dict):
            continue
        did = item.get("departmentId")
        name = item.get("departmentName") or ""
        code = item.get("departmentCode") or ""
        total_score = item.get("totalScore")
        ratio = item.get("ratio")
        score_delta = item.get("scoreDelta")

        tot_val = round(float(total_score), 2) if total_score is not None else 0.0

        item_gs = item.get("groupScores") or {}

        ckmb_val = item_gs.get("CKMB") or cg_maps.get("CKMB", {}).get(did) or cg_maps.get("CKMB", {}).get(code, 0.0)
        tdgq_val = item_gs.get("TDGQ") or cg_maps.get("TDGQ", {}).get(did) or cg_maps.get("TDGQ", {}).get(code, 0.0)
        online_val = (
            item_gs.get("CLGQ")
            or item_gs.get("ONLINE")
            or cg_maps.get("ONLINE", {}).get(did)
            or cg_maps.get("ONLINE", {}).get(code, 0.0)
        )
        tttt_val = item_gs.get("TTTT") or cg_maps.get("TTTT", {}).get(did) or cg_maps.get("TTTT", {}).get(code, 0.0)
        mdsh_val = item_gs.get("MDSH") or cg_maps.get("MDSH", {}).get(did) or cg_maps.get("MDSH", {}).get(code, 0.0)

        mdhl_fetched = item_gs.get("MDHL") or cg_maps.get("MDHL", {}).get(did) or cg_maps.get("MDHL", {}).get(code)
        if mdhl_fetched is not None:
            mdhl_val = round(float(mdhl_fetched), 2)
        else:
            known_sum = round(ckmb_val + tdgq_val + online_val + tttt_val + mdsh_val, 2)
            mdhl_val = max(0.0, round(tot_val - known_sum, 2)) if tot_val > 0 else 0.0

        group_scores = {
            "CKMB": round(float(ckmb_val), 2),
            "TDGQ": round(float(tdgq_val), 2),
            "ONLINE": round(float(online_val), 2),
            "TTTT": round(float(tttt_val), 2),
            "MDSH": round(float(mdsh_val), 2),
            "MDHL": round(float(mdhl_val), 2),
        }

        t_item = ci_maps.get("CKMB", {}).get(did) or ci_maps.get("CKMB", {}).get(code) or {}
        p_item = ci_maps.get("TDGQ", {}).get(did) or ci_maps.get("TDGQ", {}).get(code) or {}
        o_item = ci_maps.get("ONLINE", {}).get(did) or ci_maps.get("ONLINE", {}).get(code) or {}
        pay_item = ci_maps.get("TTTT", {}).get(did) or ci_maps.get("TTTT", {}).get(code) or {}
        d_item = ci_maps.get("MDSH", {}).get(did) or ci_maps.get("MDSH", {}).get(code) or {}
        sat_item = ci_maps.get("MDHL", {}).get(did) or ci_maps.get("MDHL", {}).get(code) or {}

        component_indicators = {
            "TDGQ_TienDoGiaiQuyet": {
                "totalReceived": p_item.get("totalReceived"),
                "totalOnTime": p_item.get("totalOnTime"),
                "totalOverdue": p_item.get("totalOverdue"),
                "avgProcessingDays": p_item.get("avgProcessingDays"),
                "ratio": p_item.get("ratio"),
                "score": group_scores["TDGQ"],
                "maxScore": p_item.get("maxScore", 20),
            },
            "CKMB_CongKhaiMinhBach": {
                "score": group_scores["CKMB"],
                "maxScore": t_item.get("totalMaxScore", 18),
                "ratio": t_item.get("ratio"),
                "metrics": t_item.get("metrics", []),
            },
            "MDSH_SoHoaHoSo": {
                "score": group_scores["MDSH"],
                "maxScore": d_item.get("totalMaxScore", 22),
                "ratio": d_item.get("ratio"),
                "metrics": d_item.get("metrics", []),
            },
            "ONLINE_DichVuTrucTuyen": {
                "score": group_scores["ONLINE"],
                "maxScore": o_item.get("totalMaxScore", 12),
                "ratio": o_item.get("ratio"),
            },
            "TTTT_ThanhToanTrucTuyen": {
                "score": group_scores["TTTT"],
                "maxScore": pay_item.get("totalMaxScore", 10),
                "ratio": pay_item.get("ratio"),
                "totalDossierOnlinePaymentSuccess": pay_item.get("totalDossierOnlinePaymentSuccess"),
                "totalDossierFinancialObligation": pay_item.get("totalDossierFinancialObligation"),
                "totalDossierOnlineFormalityPaymentSuccess": pay_item.get("totalDossierOnlineFormalityPaymentSuccess"),
                "totalFeeFormality": pay_item.get("totalFeeFormality"),
            },
            "MDHL_MucDoHaiLong": {
                "score": group_scores["MDHL"],
                "maxScore": sat_item.get("totalMaxScore", 18),
                "ratio": sat_item.get("ratio"),
                "totalPetitions": sat_item.get("totalPetitions"),
                "classifiedPetitions": sat_item.get("classifiedPetitions"),
                "totalDossiers": sat_item.get("totalDossiers"),
                "averageScore": sat_item.get("averageScore"),
                "metrics": sat_item.get("metrics", []),
            },
        }

        short_n = province_short_name(name)

        prov_info = {
            "departmentId": did,
            "departmentName": name,
            "shortName": short_n,
            "departmentCode": code,
            "totalScore": round(float(total_score), 2) if total_score is not None else None,
            "totalMaxScore": 100,
            "ratio": round(float(ratio), 2) if ratio is not None else None,
            "scoreDelta": round(float(score_delta), 2) if score_delta is not None else None,
            "groupScores": group_scores,
            "componentIndicators": component_indicators,
        }

        provinces.append(prov_info)

    provinces.sort(key=lambda p: (p["totalScore"] is not None, p["totalScore"] or 0), reverse=True)

    for idx, prov in enumerate(provinces, 1):
        prov["rank"] = idx

    date_str = run_date_str or datetime.now().strftime("%d%m%Y")
    period_str = f"{time_type.capitalize()} {period}/{year}" if period else f"Năm {year}"

    total_score_val = overview.get("totalScore")
    ratio_val = overview.get("ratio")
    prov_delta = overview.get("scoreDelta")

    return {
        "metadata": {
            "scope": "ProvincesDetail",
            "runDateStr": date_str,
            "timeType": time_type,
            "year": year,
            "period": period,
            "periodLabel": period_str,
            "extractedAt": utc_now(),
            "sourceUrl": ENDPOINT_SERVICE_RESULTS,
        },
        "overview": {
            "departmentName": overview.get("departmentName", "Cả nước"),
            "totalScore": round(float(total_score_val), 2) if total_score_val is not None else None,
            "totalMaxScore": overview.get("totalMaxScore", 100),
            "ratio": round(float(ratio_val), 2) if ratio_val is not None else None,
            "scoreDelta": round(float(prov_delta), 2) if prov_delta is not None else None,
            "growthPercent": data.get("growthPercent"),
            "totalProvincesCount": len(provinces),
        },
        "provinces": provinces,
    }


def find_previous_daily_date_from_index(index_detail_data: dict[str, Any], current_date_str: str, explicit_compare_date: str | None = None) -> str | None:
    """Find the previous daily date string (DDMMYYYY) from index_detail_data['availableDates']."""
    if explicit_compare_date:
        return explicit_compare_date

    avail = index_detail_data.get("availableDates", [])
    if isinstance(avail, list) and current_date_str in avail:
        idx = avail.index(current_date_str)
        if idx > 0:
            return avail[idx - 1]
    elif isinstance(avail, list) and len(avail) > 0:
        past_dates = [d for d in avail if d < current_date_str]
        if past_dates:
            return past_dates[-1]

    return None


def extract_previous_score_data_from_index(index_detail_data: dict[str, Any], prev_date_str: str) -> dict[str, Any] | None:
    """Reconstruct score_data structure for a past date from index_detail_data historical records."""
    overview_hist = index_detail_data.get("overviewHistory", {}).get(prev_date_str, {})
    provs_dict = index_detail_data.get("provinces", {})

    if not isinstance(provs_dict, dict) or not provs_dict:
        return None

    provinces: list[dict[str, Any]] = []

    for code, p_entry in provs_dict.items():
        hist = p_entry.get("history", {}).get(prev_date_str)
        if not hist:
            continue
        provinces.append({
            "departmentId": p_entry.get("departmentId"),
            "departmentName": p_entry.get("departmentName"),
            "shortName": p_entry.get("shortName"),
            "departmentCode": code,
            "totalScore": hist.get("totalScore"),
            "rank": hist.get("rank"),
            "ratio": hist.get("ratio"),
            "scoreDelta": hist.get("scoreDelta"),
            "groupScores": hist.get("groupScores") or {},
            "componentIndicators": hist.get("componentIndicators"),
        })

    if not provinces:
        return None

    provinces.sort(key=lambda p: (p["rank"] is not None, p["rank"] or 9999))

    return {
        "metadata": {
            "runDateStr": prev_date_str,
            "periodLabel": overview_hist.get("periodLabel", f"Date {prev_date_str}"),
        },
        "overview": {
            "totalScore": overview_hist.get("totalScore"),
        },
        "provinces": provinces,
    }


def compare_province_scores(
    curr_score_data: dict[str, Any],
    prev_score_data: dict[str, Any],
) -> dict[str, Any]:
    """Compare evaluation scores, daily deltas, rank changes & component indicators between current and previous snapshots."""
    curr_meta = curr_score_data["metadata"]
    prev_meta = prev_score_data["metadata"]

    curr_overview = curr_score_data["overview"]
    prev_overview = prev_score_data["overview"]

    curr_provs = {p["departmentCode"]: p for p in curr_score_data["provinces"] if p.get("departmentCode")}
    prev_provs = {p["departmentCode"]: p for p in prev_score_data["provinces"] if p.get("departmentCode")}

    curr_score_val = curr_overview.get("totalScore")
    prev_score_val = prev_overview.get("totalScore")
    national_delta = (
        round(float(curr_score_val) - float(prev_score_val), 2)
        if (curr_score_val is not None and prev_score_val is not None)
        else None
    )

    comparison_provinces: list[dict[str, Any]] = []

    increased_count = 0
    decreased_count = 0
    unchanged_count = 0
    new_provinces_count = 0

    sorted_codes = list(curr_provs.keys())
    sorted_codes.sort(key=lambda code: curr_provs[code].get("rank", 9999))

    for code in sorted_codes:
        c_prov = curr_provs[code]
        p_prov = prev_provs.get(code)

        curr_s = c_prov.get("totalScore")
        prev_s = p_prov.get("totalScore") if p_prov else None

        curr_r = c_prov.get("rank")
        prev_r = p_prov.get("rank") if p_prov else None

        c_gs = c_prov.get("groupScores") or {}
        p_gs = p_prov.get("groupScores") or {} if p_prov else {}

        gs_deltas: dict[str, float] = {}
        for g_code in ["CKMB", "TDGQ", "ONLINE", "TTTT", "MDSH", "MDHL"]:
            if g_code in c_gs and g_code in p_gs:
                gs_deltas[g_code] = round(c_gs[g_code] - p_gs[g_code], 2)

        if prev_s is None:
            delta = None
            trend = "NEW"
            trend_label = "MỚI 🆕"
            rank_change = None
            new_provinces_count += 1
        else:
            delta = round(curr_s - prev_s, 2)
            rank_change = (prev_r - curr_r) if (prev_r and curr_r) else 0

            if delta > 0:
                trend = "INCREASE"
                trend_label = "TĂNG 📈"
                increased_count += 1
            elif delta < 0:
                trend = "DECREASE"
                trend_label = "GIẢM 📉"
                decreased_count += 1
            else:
                trend = "UNCHANGED"
                trend_label = "BẰNG ➡️"
                unchanged_count += 1

        comp_record = {
            "departmentId": c_prov["departmentId"],
            "departmentName": c_prov["departmentName"],
            "shortName": c_prov.get("shortName", province_short_name(c_prov["departmentName"])),
            "departmentCode": code,
            "currentScore": curr_s,
            "previousScoreDaily": prev_s,
            "scoreDeltaDaily": delta,
            "groupScores": c_gs,
            "groupScoresDeltaDaily": gs_deltas,
            "trend": trend,
            "trendLabel": trend_label,
            "currentRank": curr_r,
            "previousRankDaily": prev_r,
            "rankChangeDaily": rank_change,
        }

        comparison_provinces.append(comp_record)

    valid_deltas = [p for p in comparison_provinces if p["scoreDeltaDaily"] is not None]
    top_improvers = sorted(valid_deltas, key=lambda p: p["scoreDeltaDaily"], reverse=True)[:5]
    top_drops = sorted(valid_deltas, key=lambda p: p["scoreDeltaDaily"])[:5]

    return {
        "metadata": {
            "scope": "ProvincesDetailComparison",
            "currentRunDate": curr_meta.get("runDateStr"),
            "previousRunDate": prev_meta.get("runDateStr", "N/A"),
            "currentPeriod": curr_meta.get("periodLabel", ""),
            "previousPeriod": prev_meta.get("periodLabel", "N/A"),
            "comparedAt": utc_now(),
        },
        "overview": {
            "currentScore": curr_score_val,
            "previousScoreDaily": prev_score_val,
            "scoreDeltaDaily": national_delta,
            "totalProvinces": len(comparison_provinces),
            "increasedCount": increased_count,
            "decreasedCount": decreased_count,
            "unchangedCount": unchanged_count,
            "newProvincesCount": new_provinces_count,
        },
        "topImproversDaily": [
            {"name": p["departmentName"], "shortName": p["shortName"], "current": p["currentScore"], "prevDaily": p["previousScoreDaily"], "deltaDaily": p["scoreDeltaDaily"]}
            for p in top_improvers if p["scoreDeltaDaily"] > 0
        ],
        "topDropsDaily": [
            {"name": p["departmentName"], "shortName": p["shortName"], "current": p["currentScore"], "prevDaily": p["previousScoreDaily"], "deltaDaily": p["scoreDeltaDaily"]}
            for p in top_drops if p["scoreDeltaDaily"] < 0
        ],
        "provinces": comparison_provinces,
    }


def update_and_save_detail_index(
    output_dir: Path,
    score_data: dict[str, Any],
    comparison_data: dict[str, Any] | None,
    date_str: str,
) -> Path:
    """Build and update data/provinces/index_detail.json (detailed index)."""
    index_detail_path = output_dir / "index_detail.json"
    index_detail_data = load_json(index_detail_path) or {}

    if not isinstance(index_detail_data, dict):
        index_detail_data = {}

    meta = score_data["metadata"]
    overview = score_data["overview"]

    avail_dates = set(index_detail_data.get("availableDates") or [])
    avail_dates.add(date_str)
    sorted_avail = sorted(list(avail_dates), key=lambda d: datetime.strptime(d, "%d%m%Y") if len(d) == 8 else d)

    allowed_detail_dates = sorted_avail[-2:]

    latest_date = date_str
    dates_before = [d for d in allowed_detail_dates if d < latest_date]
    prev_date = dates_before[-1] if dates_before else None

    index_detail_data["schemaVersion"] = 1
    index_detail_data["updatedAt"] = utc_now()
    index_detail_data["latestDate"] = latest_date
    index_detail_data["previousDate"] = prev_date
    index_detail_data["availableDates"] = allowed_detail_dates

    overview_history_detail = index_detail_data.setdefault("overviewHistory", {})
    overview_history_detail[date_str] = {
        "date": date_str,
        "periodLabel": meta.get("periodLabel"),
        "timeType": meta.get("timeType"),
        "year": meta.get("year"),
        "period": meta.get("period"),
        "totalScore": overview.get("totalScore"),
        "totalMaxScore": overview.get("totalMaxScore", 100),
        "ratio": overview.get("ratio"),
        "scoreDelta": overview.get("scoreDelta"),
        "totalProvincesCount": overview.get("totalProvincesCount"),
    }
    index_detail_data["overviewHistory"] = {d: overview_history_detail[d] for d in allowed_detail_dates if d in overview_history_detail}

    index_detail_data["latestOverview"] = overview
    index_detail_data["previousOverview"] = index_detail_data["overviewHistory"].get(prev_date) if prev_date else None
    index_detail_data["latestRankings"] = score_data.get("provinces", [])

    provinces_detail_index = index_detail_data.setdefault("provinces", {})

    for prov in score_data.get("provinces", []):
        code = prov.get("departmentCode")
        if not code:
            continue
        did = prov.get("departmentId")
        name = prov.get("departmentName")
        short_n = prov.get("shortName") or province_short_name(name)

        entry = provinces_detail_index.setdefault(code, {
            "departmentId": did,
            "departmentName": name,
            "shortName": short_n,
            "departmentCode": code,
            "latest": {},
            "previous": None,
            "history": {},
        })

        entry["departmentId"] = did
        entry["departmentName"] = name
        entry["shortName"] = short_n

        history = entry.setdefault("history", {})
        history[date_str] = {
            "rank": prov.get("rank"),
            "totalScore": prov.get("totalScore"),
            "ratio": prov.get("ratio"),
            "scoreDelta": prov.get("scoreDelta"),
            "groupScores": prov.get("groupScores"),
            "componentIndicators": prov.get("componentIndicators"),
        }

    for code, entry in provinces_detail_index.items():
        hist = entry.get("history", {})
        pruned_hist = {d: hist[d] for d in allowed_detail_dates if d in hist}
        entry["history"] = pruned_hist
        entry["latest"] = pruned_hist.get(latest_date)
        entry["previous"] = pruned_hist.get(prev_date) if prev_date else None

    if comparison_data:
        comparisons_detail = index_detail_data.setdefault("comparisons", {})
        comparisons_detail[date_str] = comparison_data
        index_detail_data["comparisons"] = {d: comparisons_detail[d] for d in allowed_detail_dates if d in comparisons_detail}

    write_json(index_detail_path, index_detail_data)

    return index_detail_path


def clean_provinces_directory_and_old_records(output_dir: Path, raw_dir: Path, max_days: int = 3) -> None:
    """Ensure data/provinces/ contains ONLY index.json, index_detail.json, and valid snapshot files, and clean records older than max_days."""
    cutoff_dt = datetime.now() - timedelta(days=max_days)
    cutoff_date_int = int(cutoff_dt.strftime("%Y%m%d"))

    print(f"\n🧹 Auto-Clean: Scanning and ensuring index & snapshot files in {output_dir} (Cutoff < {cutoff_dt.strftime('%d/%m/%Y')})...")

    removed_dates: set[str] = set()

    raw_patterns = [
        (output_dir, "scores_Provinces_*.json"),
        (output_dir, "details_Provinces_*.json"),
        (output_dir, "comparison_Provinces_*.json"),
        (output_dir, "scores_Province_*.json"),
        (output_dir, "details_Province_*.json"),
        (output_dir, "comparison_Province_*.json"),
        (raw_dir, "raw_Provinces_*.json"),
        (raw_dir / "checkpoints", "checkpoint_Provinces_*.json"),
        (raw_dir / "checkpoints", "checkpoint_national_*.json"),
    ]

    for base_dir, pattern in raw_patterns:
        if not base_dir.exists():
            continue
        for file_path in base_dir.glob(pattern):
            match = re.search(r"_(\d{8})\.json$", file_path.name)
            if match:
                date_str = match.group(1)
                try:
                    file_dt = datetime.strptime(date_str, "%d%m%Y")
                    file_date_int = int(file_dt.strftime("%Y%m%d"))
                    if file_date_int < cutoff_date_int:
                        file_path.unlink()
                        removed_dates.add(date_str)
                        print(f"   🗑️ Deleted old snapshot/comparison file: {file_path.name}")
                except ValueError:
                    pass

    valid_prefixes = (
        "comparison_Provinces_", "comparison_Province_",
        "scores_Provinces_", "scores_Province_",
        "details_Provinces_", "details_Province_",
    )
    if output_dir.exists():
        for file_path in output_dir.glob("*"):
            if file_path.is_file():
                if file_path.name in ("index.json", "index_detail.json"):
                    continue
                if file_path.name.startswith(valid_prefixes) and file_path.name.endswith(".json"):
                    continue
                file_path.unlink()
                print(f"   🗑️ Removed unneeded file: {file_path.name}")

    index_detail_path = output_dir / "index_detail.json"
    index_detail_data = load_json(index_detail_path)
    if index_detail_data and isinstance(index_detail_data, dict):
        avail = index_detail_data.get("availableDates", [])
        if isinstance(avail, list):
            prune_dates = set()
            for d in avail:
                try:
                    dt = datetime.strptime(d, "%d%m%Y")
                    if int(dt.strftime("%Y%m%d")) < cutoff_date_int:
                        prune_dates.add(d)
                except ValueError:
                    pass

            all_remove = removed_dates.union(prune_dates)
            max_allowed = 2
            potential_avail = [d for d in avail if d not in all_remove]
            if len(potential_avail) > max_allowed:
                extra_remove = set(potential_avail[:-max_allowed])
                all_remove.update(extra_remove)

            if all_remove:
                new_avail = [d for d in avail if d not in all_remove]
                index_detail_data["availableDates"] = new_avail
                latest_d = new_avail[-1] if new_avail else None
                dates_before = [d for d in new_avail if d < latest_d] if latest_d else []
                prev_d = dates_before[-1] if dates_before else None

                index_detail_data["latestDate"] = latest_d
                index_detail_data["previousDate"] = prev_d

                overview_hist = index_detail_data.get("overviewHistory", {})
                if isinstance(overview_hist, dict):
                    for d in all_remove:
                        overview_hist.pop(d, None)

                index_detail_data["latestOverview"] = overview_hist.get(latest_d) if latest_d else None
                index_detail_data["previousOverview"] = overview_hist.get(prev_d) if prev_d else None

                comparisons = index_detail_data.get("comparisons", {})
                if isinstance(comparisons, dict):
                    for d in all_remove:
                        comparisons.pop(d, None)

                provs_idx = index_detail_data.get("provinces", {})
                if isinstance(provs_idx, dict):
                    for code, p_data in provs_idx.items():
                        history = p_data.get("history", {})
                        if isinstance(history, dict):
                            for d in all_remove:
                                history.pop(d, None)
                        p_data["latest"] = history.get(latest_d) if latest_d else None
                        p_data["previous"] = history.get(prev_d) if prev_d else None

                write_json(index_detail_path, index_detail_data)
                print(f"   📌 Pruned {len(all_remove)} old date entries from {index_detail_path.name}")

    print(f"   ✅ Clean complete: {output_dir} contains index_detail.json and snapshot comparison files.")


def print_province_scores_table(score_data: dict[str, Any], comparison_data: dict[str, Any] | None = None) -> None:
    """Print beautifully formatted ASCII/Unicode summary table of province rankings and component indicators."""
    meta = score_data["metadata"]
    overview = score_data["overview"]
    provinces = score_data["provinces"]

    print("\n" + "=" * 110)
    print(f"📊 BẢNG XẾP HẠNG VÀ CHI TIẾT DVCQG UBND TỈNH / THÀNH PHỐ TOÀN QUỐC ({meta['periodLabel']})")
    print(f"📅 Mốc Ngày Trích Xuất: {meta['runDateStr']} | Tổng số Tỉnh/Thành phố: {overview['totalProvincesCount']}")
    print(f"🏆 Điểm Trung Bình Cả Nước: {overview.get('totalScore', 'N/A')}/100")
    print("=" * 110)

    header = f"{'XH':<4} | {'Mã':<5} | {'Tỉnh / Thành phố':<25} | {'Tổng Điểm':<10} | {'CKMB':<6} | {'TDGQ':<6} | {'DVTTH':<6} | {'TTTT':<6} | {'MDHL':<6} | {'MDSH':<6} | {'Shift':<7}"
    print(header)
    print("-" * 110)

    comp_map = {}
    if comparison_data:
        comp_map = {p["departmentCode"]: p for p in comparison_data.get("provinces", [])}

    for p in provinces:
        rank = p.get("rank", 0)
        code = p.get("departmentCode", "")
        name = p.get("shortName") or province_short_name(p.get("departmentName", ""))
        tot = f"{p.get('totalScore', 0.0):.2f}"
        gs = p.get("groupScores", {})

        ckmb = f"{gs.get('CKMB', 0.0):.2f}"
        tdgq = f"{gs.get('TDGQ', 0.0):.2f}"
        online = f"{gs.get('ONLINE', 0.0):.2f}"
        tttt = f"{gs.get('TTTT', 0.0):.2f}"
        mdhl = f"{gs.get('MDHL', 0.0):.2f}"
        mdsh = f"{gs.get('MDSH', 0.0):.2f}"

        c_info = comp_map.get(code, {})
        rc = c_info.get("rankChangeDaily")
        if rc is None:
            shift_str = "NEW"
        elif rc > 0:
            shift_str = f"▲{rc}"
        elif rc < 0:
            shift_str = f"▼{abs(rc)}"
        else:
            shift_str = "="

        print(f"{rank:<4} | {code:<5} | {name:<25} | {tot:<10} | {ckmb:<6} | {tdgq:<6} | {online:<6} | {tttt:<6} | {mdhl:<6} | {mdsh:<6} | {shift_str:<7}")

    print("-" * 110)

    if comparison_data:
        comp_meta = comparison_data["metadata"]
        comp_ov = comparison_data["overview"]
        print(f"📈 CHÊNH LỆCH SO VỚI NGÀY TRƯỚC ĐÓ ({comp_meta['previousRunDate']} -> {comp_meta['currentRunDate']}):")
        print(f"   • Tăng điểm/thứ hạng: {comp_ov['increasedCount']} Tỉnh/TP 📈")
        print(f"   • Giảm điểm/thứ hạng: {comp_ov['decreasedCount']} Tỉnh/TP 📉")
        print(f"   • Giữ nguyên        : {comp_ov['unchangedCount']} Tỉnh/TP ➡️")
        if comp_ov.get("newProvincesCount"):
            print(f"   • Tỉnh/TP Mới      : {comp_ov['newProvincesCount']} 🆕")

        top_imp = comparison_data.get("topImproversDaily", [])
        if top_imp:
            print("\n🌟 TOP 5 TỈNH/THÀNH PHỐ TĂNG ĐIỂM MẠNH NHẤT TRONG NGÀY:")
            for idx, item in enumerate(top_imp, 1):
                print(f"   {idx}. {item['shortName']} ({item['name']}): {item['prevDaily']} ➡️ {item['current']} (Tăng +{item['deltaDaily']:.2f} điểm)")

        top_drp = comparison_data.get("topDropsDaily", [])
        if top_drp:
            print("\n⚠️ TOP 5 TỈNH/THÀNH PHỐ GIẢM ĐIỂM TRONG NGÀY:")
            for idx, item in enumerate(top_drp, 1):
                print(f"   {idx}. {item['shortName']} ({item['name']}): {item['prevDaily']} ➡️ {item['current']} (Giảm {item['deltaDaily']:.2f} điểm)")

    print("=" * 110 + "\n")


def main() -> int:
    default_year = datetime.now().year
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--time-type", choices=["year", "month", "quarter"], default="year", help="Time type filter (default: year)")
    parser.add_argument("--year", type=int, default=default_year, help=f"Evaluation year (default: {default_year})")
    parser.add_argument("--period", type=int, default=None, help="Month (1-12) or Quarter (1-4) period")
    parser.add_argument("--date", type=str, default=None, help="Explicit run date string in DDMMYYYY format")
    parser.add_argument("--compare-date", type=str, default=None, help="Explicit previous date DDMMYYYY to compare against")
    parser.add_argument("--clean-days", type=int, default=3, help="Auto-clean snapshots older than N days (default: 3)")
    parser.add_argument("--output-dir", type=str, default=str(DEFAULT_OUTPUT_DIR), help="Output directory for province JSON files")
    parser.add_argument("--raw-dir", type=str, default=str(DEFAULT_RAW_DIR), help="Raw output directory")
    parser.add_argument("--concurrency", type=int, default=3, help="Concurrent threads for fetching 6 component groups (default: 3)")
    parser.add_argument("--timeout", type=int, default=90, help="HTTP request timeout in seconds (default: 90)")
    parser.add_argument("--max-retries", type=int, default=8, help="Max retries for HTTP requests (default: 8)")
    parser.add_argument("--skip-clean", action="store_true", help="Skip auto-cleaning old snapshot files")
    parser.add_argument("--force", action="store_true", help="Bắt buộc crawl mới từ DVCQG, bỏ qua cache/checkpoint trên đĩa")
    args = parser.parse_args()

    if args.time_type == "year":
        args.period = None
    elif args.time_type == "month" and args.period is None:
        args.period = 3
    elif args.time_type == "quarter" and args.period is None:
        args.period = 1

    run_date_str = args.date or datetime.now().strftime("%d%m%Y")
    year_str = str(args.year)

    output_dir = Path(args.output_dir)
    raw_dir = Path(args.raw_dir) / year_str / "provinces"
    checkpoint_dir = raw_dir / "checkpoints"

    output_dir.mkdir(parents=True, exist_ok=True)
    raw_dir.mkdir(parents=True, exist_ok=True)
    checkpoint_dir.mkdir(parents=True, exist_ok=True)

    period_tag = f"year_{args.year}" if args.time_type == "year" else f"{args.time_type}_{args.period}_{args.year}"
    checkpoint_file = checkpoint_dir / f"checkpoint_Provinces_{period_tag}_{run_date_str}.json"
    checkpoint_national = checkpoint_dir / f"checkpoint_national_{period_tag}_{run_date_str}.json"
    raw_file = raw_dir / f"raw_Provinces_{period_tag}_{run_date_str}.json"

    if args.force:
        checkpoint_file.unlink(missing_ok=True)
        checkpoint_national.unlink(missing_ok=True)
        raw_file.unlink(missing_ok=True)
        print("⚡ Chế độ --force: Đã làm mới cache, bắt buộc tải dữ liệu mới nhất từ Cổng DVCQG.")

    # Step 1: Clean old files
    if not args.skip_clean and args.clean_days > 0:
        clean_provinces_directory_and_old_records(output_dir, raw_dir, max_days=args.clean_days)

    index_detail_path = output_dir / "index_detail.json"
    index_detail_data = load_json(index_detail_path) or {}

    print(f"\n🚀 Khởi động trích xuất Dữ liệu Chi tiết Chỉ số Thành phần UBND Tỉnh/Thành phố ({args.time_type.upper()} {args.period or ''}/{args.year})...")

    # Proactive Session Warmup & Anti-WAF TLS Handshake
    print("🌐 Khởi tạo kết nối & nhận diện phiên làm việc DVCQG (Session Warmup)...")
    if warmup_session(timeout=min(20, args.timeout)):
        print("✅ Thiết lập phiên TLS & Cookie DVCQG thành công.")
    else:
        print("⚠️ Không thể warmup session trước; tiếp tục với direct request...")

    pbar = CrawlerProgressBar(total=7, desc="Crawling All Provinces Detail", unit="step")

    # Step 2: Fetch national service-results
    pbar.set_postfix_str("Fetching National Service Results...")
    try:
        raw_national = fetch_all_provinces_service_results(
            args.time_type,
            args.year,
            args.period,
            checkpoint_file=checkpoint_national,
            raw_file=raw_file,
            timeout=args.timeout,
            max_retries=args.max_retries,
        )
        pbar.update(1, status="National OK")
    except Exception as exc:
        pbar.close()
        print(f"\n❌ ERROR: Failed to fetch national service results: {exc}", file=sys.stderr)
        return 1

    write_json(raw_file, raw_national)
    write_json(checkpoint_national, raw_national)

    # Step 3: Fetch 6 component group score maps
    group_maps, group_item_maps = fetch_provinces_component_groups_maps(
        args.time_type,
        args.year,
        args.period,
        checkpoint_file=checkpoint_file,
        timeout=args.timeout,
        max_retries=args.max_retries,
        concurrency=args.concurrency,
        pbar=pbar,
    )
    pbar.close()

    # Step 4: Extract detailed score data
    print("🧩 Ghép nối 6 nhóm chỉ tiêu thành phần & sub-metrics, tính toán Xếp hạng Rank Tỉnh/Thành phố...")
    score_data = extract_province_score_data(
        raw_national,
        args.time_type,
        args.year,
        args.period,
        component_group_maps=group_maps,
        component_group_item_maps=group_item_maps,
        run_date_str=run_date_str,
    )

    details_file = output_dir / f"details_Provinces_{run_date_str}.json"
    details_file_alt = output_dir / f"details_Province_{run_date_str}.json"
    write_json(details_file, score_data)
    write_json(details_file_alt, score_data)
    print(f"✅ Đã tạo & lưu file Chi tiết chỉ số Tỉnh/TP theo Ngày: {details_file}")

    # Step 5: Comparison
    prev_date_str = find_previous_daily_date_from_index(index_detail_data, run_date_str, explicit_compare_date=args.compare_date)
    comparison_data = None

    if prev_date_str:
        prev_score_data = extract_previous_score_data_from_index(index_detail_data, prev_date_str)
        if prev_score_data:
            print(f"📈 Engine So sánh Điểm Đồng bộ Kỳ Liền ({run_date_str} vs {prev_date_str})...")
            comparison_data = compare_province_scores(score_data, prev_score_data)
        else:
            print(f"ℹ️  Không tìm thấy dữ liệu mốc ngày {prev_date_str}. Khởi tạo dữ liệu so sánh kỳ đầu tiên.")
            prev_score_data = {
                "metadata": {"runDateStr": "N/A", "periodLabel": "N/A"},
                "overview": {},
                "provinces": [],
            }
            comparison_data = compare_province_scores(score_data, prev_score_data)
    else:
        print("ℹ️  Chưa tìm thấy mốc ngày chạy trước đó. Khởi tạo dữ liệu so sánh kỳ đầu tiên.")
        prev_score_data = {
            "metadata": {"runDateStr": "N/A", "periodLabel": "N/A"},
            "overview": {},
            "provinces": [],
        }
        comparison_data = compare_province_scores(score_data, prev_score_data)

    # Step 6: Update index_detail.json
    master_detail_file = update_and_save_detail_index(output_dir, score_data, comparison_data, run_date_str)
    print(f"🚀 Saved Master API Index Database (Detailed): {master_detail_file}")

    # Step 7: Clean old records
    clean_provinces_directory_and_old_records(output_dir, raw_dir, max_days=args.clean_days)

    # Step 8: Print console summary table
    print_province_scores_table(score_data, comparison_data)

    print("🏁 Hoàn thành xuất sắc nhiệm vụ crawl dữ liệu chi tiết Tỉnh / Thành phố cho index_detail.json!")
    return 0


if __name__ == "__main__":
    sys.exit(main())
