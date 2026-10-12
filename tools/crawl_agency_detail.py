#!/usr/bin/env python3
"""
Tool crawl_agency_detail.py: Trích xuất 6 nhóm chỉ số thành phần & sub-metrics chi tiết, duy trì CSDL Master Index Detail (index_detail.json) cho tất cả Sở/Ngành (Agencies) của 34 UBND Tỉnh / Thành phố toàn quốc từ DVCQG.

Endpoint API công khai DVCQG:
1. API Dữ liệu tổng hợp        : https://dichvucong.gov.vn/api/v1/reporting/evaluation/service-results
2. API Công khai minh bạch    : https://dichvucong.gov.vn/api/v1/reporting/evaluation/transparency
3. API Tiến độ giải quyết     : https://dichvucong.gov.vn/api/v1/reporting/evaluation/dvc-progress-tree
4. API Dịch vụ công trực tuyến: https://dichvucong.gov.vn/api/v1/reporting/evaluation/provide-online-tree
5. API Thanh toán trực tuyến  : https://dichvucong.gov.vn/api/v1/reporting/evaluation/formality-online-payment-tree
6. API Số hóa hồ sơ           : https://dichvucong.gov.vn/api/v1/reporting/evaluation/dossier-digitized
7. API Mức độ hài lòng       : https://dichvucong.gov.vn/api/v1/reporting/evaluation/handling-satisfaction

Tính năng chính:
1. Trích xuất chi tiết 6 nhóm chỉ số thành phần (CKMB, TDGQ, ONLINE, TTTT, MDSH, MDHL) & sub-metrics của tất cả Sở/Ngành toàn quốc.
2. Lưu file CSDL Master Index Detail (index_detail.json) và snapshot chi tiết theo mốc ngày DDMMYYYY trong data/agencies/:
   - index_detail.json              : API Detailed Index Database chứa toàn bộ số liệu chi tiết các chỉ số thành phần (componentIndicators) phục vụ tra cứu.
   - details_Agencies_DDMMYYYY.json : Dữ liệu chi tiết 6 nhóm chỉ tiêu & sub-metrics tất cả Sở/Ngành toàn quốc theo ngày.
3. Checkpoint Resumption theo từng tỉnh tại data/raw/<year>/agencies/checkpoints/.
4. Cơ chế chống WAF/Firewall: User-Agent Rotation Pool, Header ngẫu nhiên, TLS Session Cookie Warmup & Randomized Sleep Jitter.
5. Engine Tự động dọn dẹp Snapshot & Index cũ (> N ngày, mặc định: 3 ngày / 72 giờ).

Sử dụng:
  python tools/crawl_agency_detail.py --time-type year --year 2026
  python tools/crawl_agency_detail.py --time-type month --year 2026 --period 3
  python tools/crawl_agency_detail.py --provinces H01,H21
  python tools/crawl_agency_detail.py --clean-days 3
"""

from __future__ import annotations

import argparse
import gzip
import http.cookiejar
import json
import random
import re
import socket
import ssl
import sys
import threading
import time
import urllib.error
import urllib.request
import zlib
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

# Ensure UTF-8 console output across all platforms
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

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
DEFAULT_OUTPUT_DIR = DATA_DIR / "agencies"
DEFAULT_RAW_DIR = DATA_DIR / "raw"
PROVINCES_CONFIG = DATA_DIR / "config" / "provinces.json"

try:
    from tqdm import tqdm
    HAS_TQDM = True
except ImportError:
    HAS_TQDM = False


def generate_random_user_agent(
    device_type=None,
    browser_type=None,
    chrome_versions=(125, 138),
    firefox_versions=(120, 135),
) -> str:
    """Generate a randomized desktop or mobile User-Agent string across OS and browser types."""
    if not device_type:
        device_type = random.choice(["android", "ios", "windows", "ubuntu", "macos"])

    if not browser_type:
        browser_type = random.choice(["chrome", "firefox"])

    if browser_type == "chrome":
        chrome_ver_list = list(range(chrome_versions[0], chrome_versions[1]))
        major_version = random.choice(chrome_ver_list)
        minor_version = random.randint(0, 9)
        build_version = random.randint(1000, 9999)
        patch_version = random.randint(0, 99)
        browser_version = f"{major_version}.{minor_version}.{build_version}.{patch_version}"
    elif browser_type == "firefox":
        firefox_ver_list = list(range(firefox_versions[0], firefox_versions[1]))
        browser_version = str(random.choice(firefox_ver_list))
    else:
        browser_version = "125.0.0.0"

    if device_type == "android":
        android_versions = ["11.0", "12.0", "13.0", "14.0", "15.0"]
        android_device = random.choice(["SM-G998B", "Pixel 7 Pro", "Pixel 8", "Xiaomi 13", "SM-S918B"])
        if browser_type == "chrome":
            return f"Mozilla/5.0 (Linux; Android {random.choice(android_versions)}; {android_device}) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{browser_version} Mobile Safari/537.36"
        return f"Mozilla/5.0 (Android {random.choice(android_versions)}; Mobile; rv:{browser_version}.0) Gecko/{browser_version}.0 Firefox/{browser_version}.0"

    if device_type == "ios":
        ios_versions = ["16_5", "16_6", "17_0", "17_2", "17_4"]
        ios_ver = random.choice(ios_versions)
        return f"Mozilla/5.0 (iPhone; CPU iPhone OS {ios_ver} like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1"

    if device_type == "windows":
        if browser_type == "chrome":
            return f"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{browser_version} Safari/537.36"
        return f"Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:{browser_version}.0) Gecko/20100101 Firefox/{browser_version}.0"

    if device_type == "macos":
        if browser_type == "chrome":
            return f"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{browser_version} Safari/537.36"
        return f"Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:{browser_version}.0) Gecko/20100101 Firefox/{browser_version}.0"

    # Default Ubuntu Linux
    if browser_type == "chrome":
        return f"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{browser_version} Safari/537.36"
    return f"Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:{browser_version}.0) Gecko/20100101 Firefox/{browser_version}.0"


def get_random_headers() -> dict[str, str]:
    """Build random browser headers mimicking modern Google Chrome or Firefox."""
    ua = generate_random_user_agent()
    is_mobile = "Mobile" in ua
    platform = '"Windows"'
    if "Android" in ua:
        platform = '"Android"'
    elif "iPhone" in ua:
        platform = '"iOS"'
    elif "Macintosh" in ua:
        platform = '"macOS"'
    elif "Linux" in ua:
        platform = '"Linux"'

    headers = {
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json",
        "User-Agent": ua,
        "Accept-Language": random.choice([
            "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
            "vi,en-US;q=0.9,en;q=0.8",
            "vi-VN,vi;q=0.9,fr-FR;q=0.8,fr;q=0.7,en-US;q=0.6,en;q=0.5",
        ]),
        "Accept-Encoding": "gzip, deflate, br",
        "Referer": "https://dichvucong.gov.vn/danh-gia-chat-luong-phuc-vu",
        "Origin": "https://dichvucong.gov.vn",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
    }
    if "Chrome" in ua or "CriOS" in ua:
        ver_match = re.search(r"(?:Chrome|CriOS)/(\d+)", ua)
        ver = ver_match.group(1) if ver_match else "125"
        headers["Sec-Ch-Ua"] = f'"Chromium";v="{ver}", "Google Chrome";v="{ver}", "Not-A.Brand";v="99"'
        headers["Sec-Ch-Ua-Mobile"] = "?1" if is_mobile else "?0"
        headers["Sec-Ch-Ua-Platform"] = platform
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


class CrawlerProgressBar:
    """Flexible thread-safe progress bar supporting tqdm with fallback to clean standard console output."""

    def __init__(self, total: int, desc: str = "Crawling Details", unit: str = "tỉnh"):
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
                if status:
                    self.pbar.set_postfix_str(status)
            else:
                self._render_fallback(status)

    def _render_fallback(self, status: str = ""):
        pct = (self.current / self.total * 100) if self.total > 0 else 0
        filled_len = int(25 * self.current // self.total) if self.total > 0 else 0
        bar = "█" * filled_len + "░" * (25 - filled_len)
        post = f" | {status}" if status else ""
        sys.stdout.write(f"\r  [{bar}] {self.current}/{self.total} ({pct:.1f}%){post}")
        sys.stdout.flush()
        if self.current >= self.total:
            sys.stdout.write("\n")
            sys.stdout.flush()

    def close(self):
        if self.pbar:
            self.pbar.close()


def utc_now() -> str:
    """Return ISO format UTC timestamp string."""
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def load_json(path: Path | None, default: Any = None) -> Any:
    """Safely load JSON from file path with fallback."""
    if not path or not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        print(f"⚠️ Warning loading JSON from {path}: {exc}", file=sys.stderr)
        return default


def write_json(path: Path, data: Any, indent: int = 2) -> None:
    """Safely write JSON to disk atomically."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(f".tmp_{random.randint(1000, 9999)}")
    try:
        temp_path.write_text(json.dumps(data, ensure_ascii=False, indent=indent), encoding="utf-8")
        temp_path.replace(path)
    except Exception:
        if temp_path.exists():
            temp_path.unlink(missing_ok=True)
        raise


def decompress_body(raw_bytes: bytes, encoding: str | None) -> str:
    """Decompress gzip, deflate or brotli response stream."""
    if not encoding:
        return raw_bytes.decode("utf-8", errors="replace")
    encoding = encoding.lower()
    try:
        if "gzip" in encoding:
            return gzip.decompress(raw_bytes).decode("utf-8", errors="replace")
        if "deflate" in encoding:
            return zlib.decompress(raw_bytes, -zlib.MAX_WBITS).decode("utf-8", errors="replace")
        if "br" in encoding:
            import brotli  # type: ignore
            return brotli.decompress(raw_bytes).decode("utf-8", errors="replace")
    except Exception:
        pass
    return raw_bytes.decode("utf-8", errors="replace")


def is_checkpoint_valid(checkpoint_path: Path | None, max_age_seconds: int = 86400) -> bool:
    """Check if checkpoint file exists and is within max_age_seconds (default 24 hours)."""
    if not checkpoint_path or not checkpoint_path.exists():
        return False
    try:
        mtime = checkpoint_path.stat().st_mtime
        if (time.time() - mtime) > max_age_seconds:
            checkpoint_path.unlink(missing_ok=True)
            return False
        return True
    except Exception:
        return False


def fetch_dvc_endpoint(
    url: str,
    time_type: str,
    year: int,
    period: int | None = None,
    root_dept_id: str | None = None,
    page_size: int = 300,
    current_page: int = 1,
    timeout: int = 45,
    max_retries: int = 6,
    delay_min: float = 0.5,
    delay_max: float = 1.5,
) -> dict[str, Any]:
    """Fetch DVCQG API endpoint with session cookie reuse, gzip decompression, and smart backoff retries."""
    global _SESSION_WARMED_UP
    payload: dict[str, Any] = {
        "timeType": time_type,
        "year": year,
        "pageSize": page_size,
        "currentPage": current_page,
    }
    if root_dept_id:
        payload["rootDepartmentId"] = root_dept_id

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
    sleep_sec = random.uniform(delay_min, delay_max) + random.uniform(0.05, 0.20)
    time.sleep(sleep_sec)

    for attempt in range(1, max_retries + 1):
        if not _SESSION_WARMED_UP:
            warmup_session(timeout=timeout)

        headers = get_random_headers()
        request = urllib.request.Request(url, data=body, method="POST", headers=headers)
        try:
            with OPENER.open(request, timeout=timeout) as response:
                status = response.status
                encoding = response.headers.get("Content-Encoding")
                raw_bytes = response.read()
                raw_text = decompress_body(raw_bytes, encoding)
                parsed = json.loads(raw_text)
                if status not in (200, 201):
                    raise RuntimeError(f"API returned HTTP status {status}")
                return parsed
        except urllib.error.HTTPError as exc:
            err_body = exc.read().decode("utf-8", errors="replace")
            if exc.code == 403:
                warmup_session(timeout=timeout, force=True)
            if exc.code in (403, 429, 500, 502, 503, 504) and attempt < max_retries:
                retry_wait = (2.0 * (1.3 ** (attempt - 1))) + random.uniform(0.5, 2.0)
                time.sleep(retry_wait)
                continue
            raise RuntimeError(f"HTTP Error {exc.code}: {err_body[:300]}") from exc
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            reason = getattr(exc, "reason", str(exc))
            if attempt < max_retries:
                retry_wait = (2.0 * (1.3 ** (attempt - 1))) + random.uniform(0.5, 2.0)
                time.sleep(retry_wait)
                continue
            raise RuntimeError(f"Network error connecting to DVCQG: {reason}") from exc

    raise RuntimeError(f"Exceeded max retries ({max_retries}) connecting to {url}")


def load_provinces_list(filter_codes: list[str] | None = None) -> list[dict[str, Any]]:
    """Load the list of 34 provinces/cities from data/config/provinces.json or discover them."""
    provinces: list[dict[str, Any]] = []
    if PROVINCES_CONFIG.exists():
        cfg = load_json(PROVINCES_CONFIG, {})
        provinces = cfg.get("provinces", [])

    if not provinces:
        dept_cfg = DATA_DIR / "config" / "departments.json"
        if dept_cfg.exists():
            cfg = load_json(dept_cfg, {})
            provinces = cfg.get("unclassified", [])

    if filter_codes:
        normalized_codes = {c.strip().upper() for c in filter_codes if c.strip()}
        provinces = [p for p in provinces if p.get("departmentCode", "").upper() in normalized_codes]

    clean_list = []
    for p in provinces:
        p_id = p.get("rootDepartmentId") or p.get("departmentId")
        p_code = p.get("departmentCode")
        p_name = p.get("departmentName")
        if p_id and p_code:
            clean_list.append({
                "departmentId": p_id,
                "rootDepartmentId": p_id,
                "departmentCode": p_code,
                "departmentName": p_name or f"UBND tỉnh {p_code}",
            })
    clean_list.sort(key=lambda x: x["departmentCode"])
    return clean_list


def fetch_all_province_detail_endpoints(
    root_dept_id: str,
    time_type: str,
    year: int,
    period: int | None = None,
    timeout: int = 45,
    max_retries: int = 5,
) -> dict[str, dict[str, Any]]:
    """Fetch the 7 detailed evaluation endpoints for a single province."""
    tasks = [
        ("SERVICE_RESULTS", ENDPOINT_SERVICE_RESULTS),
        ("TRANSPARENCY", ENDPOINT_TRANSPARENCY),
        ("DIGITIZED", ENDPOINT_DIGITIZED),
        ("PROGRESS", ENDPOINT_PROGRESS),
        ("ONLINE", ENDPOINT_ONLINE),
        ("PAYMENT", ENDPOINT_PAYMENT),
        ("HANDLING_SATISFACTION", ENDPOINT_HANDLING_SATISFACTION),
    ]

    results: dict[str, dict[str, Any]] = {}
    for code, url in tasks:
        try:
            resp = fetch_dvc_endpoint(
                url,
                time_type=time_type,
                year=year,
                period=period,
                root_dept_id=root_dept_id,
                page_size=300,
                current_page=1,
                timeout=timeout,
                max_retries=max_retries,
            )
            results[code] = resp.get("data", {}) if isinstance(resp, dict) else {}
        except Exception as exc:
            print(f"\n⚠️ Cảnh báo: Lỗi tải endpoint {code} cho rootDepartmentId={root_dept_id}: {exc}", file=sys.stderr)
            results[code] = {}

    return results


def process_province_agencies_details(
    province: dict[str, Any],
    endpoints_data: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    """Parse and assemble complete 6 component indicators breakdown for all agencies in a province."""
    p_code = province["departmentCode"]
    p_name = province["departmentName"]
    root_id = province["rootDepartmentId"]

    sr_raw = endpoints_data.get("SERVICE_RESULTS", {})
    trans_raw = endpoints_data.get("TRANSPARENCY", {})
    digi_raw = endpoints_data.get("DIGITIZED", {})
    prog_raw = endpoints_data.get("PROGRESS", {})
    online_raw = endpoints_data.get("ONLINE", {})
    pay_raw = endpoints_data.get("PAYMENT", {})
    sat_raw = endpoints_data.get("HANDLING_SATISFACTION", {})

    evaluation_list = sr_raw.get("evaluation") or []

    # Map departmentId to item dictionary for each API
    trans_map = {item["departmentId"]: item for item in trans_raw.get("evaluation", []) if isinstance(item, dict) and item.get("departmentId")}
    digi_map = {item["departmentId"]: item for item in digi_raw.get("evaluation", []) if isinstance(item, dict) and item.get("departmentId")}
    prog_map = {item["departmentId"]: item for item in prog_raw.get("children", []) if isinstance(item, dict) and item.get("departmentId")}
    online_map = {item["departmentId"]: item for item in online_raw.get("children", []) if isinstance(item, dict) and item.get("departmentId")}
    pay_map = {item["departmentId"]: item for item in pay_raw.get("children", []) if isinstance(item, dict) and item.get("departmentId")}
    sat_map = {item["departmentId"]: item for item in sat_raw.get("evaluation", []) if isinstance(item, dict) and item.get("departmentId")}

    agencies_detail: list[dict[str, Any]] = []

    for item in evaluation_list:
        if not isinstance(item, dict):
            continue
        if item.get("childGroup") != "AGENCY":
            continue

        did = item.get("departmentId")
        if not did:
            continue

        name = item.get("departmentName")
        code = item.get("departmentCode")
        total_score = round(float(item.get("totalScore", 0.0)), 2) if item.get("totalScore") is not None else 0.0

        # Detailed component criteria mappings
        t_info = trans_map.get(did, {})
        d_info = digi_map.get(did, {})
        p_info = prog_map.get(did, {})
        o_info = online_map.get(did, {})
        pay_info = pay_map.get(did, {})
        sat_info = sat_map.get(did, {})

        ckmb_score = round(float(t_info.get("totalScore", 0.0)), 2)
        tdgq_score = round(float(p_info.get("score", p_info.get("totalScore", 0.0))), 2)
        online_score = round(float(o_info.get("totalScore", 0.0)), 2)
        tttt_score = round(float(pay_info.get("totalScore", 0.0)), 2)
        mdsh_score = round(float(d_info.get("totalScore", 0.0)), 2)

        if sat_info.get("totalScore") is not None:
            mdhl_score = round(float(sat_info.get("totalScore", 0.0)), 2)
        else:
            known_sum = round(ckmb_score + tdgq_score + online_score + tttt_score + mdsh_score, 2)
            mdhl_score = max(0.0, round(total_score - known_sum, 2)) if total_score > 0 else 0.0

        agency_record = {
            "departmentId": did,
            "departmentName": name,
            "departmentCode": code,
            "childGroup": "AGENCY",
            "provinceCode": p_code,
            "provinceName": p_name,
            "rootDepartmentId": root_id,
            "totalScore": total_score,
            "totalMaxScore": 100,
            "ratio": round(float(item.get("ratio", 0.0)), 2) if item.get("ratio") is not None else None,
            "scoreDelta": round(float(item.get("scoreDelta", 0.0)), 2) if item.get("scoreDelta") is not None else None,
            "groupScores": {
                "CKMB": ckmb_score,
                "TDGQ": tdgq_score,
                "ONLINE": online_score,
                "TTTT": tttt_score,
                "MDSH": mdsh_score,
                "MDHL": mdhl_score,
            },
            "componentIndicators": {
                "TDGQ_TienDoGiaiQuyet": {
                    "totalReceived": p_info.get("totalReceived"),
                    "totalOnTime": p_info.get("totalOnTime"),
                    "totalOverdue": p_info.get("totalOverdue"),
                    "avgProcessingDays": p_info.get("avgProcessingDays"),
                    "ratio": p_info.get("ratio"),
                    "score": tdgq_score,
                    "maxScore": p_info.get("maxScore", 20),
                },
                "CKMB_CongKhaiMinhBach": {
                    "score": ckmb_score,
                    "maxScore": t_info.get("totalMaxScore", 18),
                    "ratio": t_info.get("ratio"),
                    "metrics": t_info.get("metrics", []),
                },
                "MDSH_SoHoaHoSo": {
                    "score": mdsh_score,
                    "maxScore": d_info.get("totalMaxScore", 22),
                    "ratio": d_info.get("ratio"),
                    "metrics": d_info.get("metrics", []),
                },
                "ONLINE_DichVuTrucTuyen": {
                    "score": online_score,
                    "maxScore": o_info.get("totalMaxScore", 12),
                    "ratio": o_info.get("ratio"),
                },
                "TTTT_ThanhToanTrucTuyen": {
                    "score": tttt_score,
                    "maxScore": pay_info.get("totalMaxScore", 10),
                    "ratio": pay_info.get("ratio"),
                    "totalDossierOnlinePaymentSuccess": pay_info.get("totalDossierOnlinePaymentSuccess"),
                    "totalDossierFinancialObligation": pay_info.get("totalDossierFinancialObligation"),
                    "totalDossierOnlineFormalityPaymentSuccess": pay_info.get("totalDossierOnlineFormalityPaymentSuccess"),
                    "totalFeeFormality": pay_info.get("totalFeeFormality"),
                },
                "MDHL_MucDoHaiLong": {
                    "score": mdhl_score,
                    "maxScore": sat_info.get("totalMaxScore", 18),
                    "ratio": sat_info.get("ratio"),
                    "totalPetitions": sat_info.get("totalPetitions"),
                    "classifiedPetitions": sat_info.get("classifiedPetitions"),
                    "totalDossiers": sat_info.get("totalDossiers"),
                    "averageScore": sat_info.get("averageScore"),
                    "metrics": sat_info.get("metrics", []),
                },
            },
        }
        agencies_detail.append(agency_record)

    # Sort agencies in this province descending
    agencies_detail.sort(key=lambda u: (u["totalScore"] is not None, u["totalScore"] or 0), reverse=True)
    for idx, u in enumerate(agencies_detail, 1):
        u["groupRank"] = idx

    return agencies_detail


def fetch_province_agencies_detail_pipeline(
    province: dict[str, Any],
    time_type: str,
    year: int,
    period: int | None = None,
    checkpoint_dir: Path | None = None,
    run_date_str: str | None = None,
    timeout: int = 45,
    force: bool = False,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Pipeline to fetch details for a province with checkpoint resumption."""
    p_code = province["departmentCode"]
    date_str = run_date_str or datetime.now().strftime("%d%m%Y")
    checkpoint_file = checkpoint_dir / f"checkpoint_detail_{p_code}_{date_str}.json" if checkpoint_dir else None

    if not force and is_checkpoint_valid(checkpoint_file):
        cached = load_json(checkpoint_file)
        if cached and isinstance(cached, dict) and "agenciesDetail" in cached:
            return province, cached.get("agenciesDetail", [])

    endpoints_data = fetch_all_province_detail_endpoints(
        root_dept_id=province["rootDepartmentId"],
        time_type=time_type,
        year=year,
        period=period,
        timeout=timeout,
    )

    agencies_detail = process_province_agencies_details(province, endpoints_data)

    if checkpoint_file:
        write_json(checkpoint_file, {
            "provinceCode": p_code,
            "provinceName": province["departmentName"],
            "rootDepartmentId": province["rootDepartmentId"],
            "agencyCount": len(agencies_detail),
            "agenciesDetail": agencies_detail,
            "updatedAt": utc_now(),
        })

    return province, agencies_detail


def crawl_all_agencies_detail(
    provinces: list[dict[str, Any]],
    time_type: str,
    year: int,
    period: int | None = None,
    checkpoint_dir: Path | None = None,
    run_date_str: str | None = None,
    concurrency: int = 3,
    timeout: int = 45,
    force: bool = False,
) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
    """Crawl detailed component indicators for all agencies across all provinces."""
    all_agencies_detail: list[dict[str, Any]] = []
    by_province: dict[str, dict[str, Any]] = {}

    pbar = CrawlerProgressBar(total=len(provinces), desc="Crawling Detail", unit="tỉnh")
    warmup_session(timeout=timeout)

    lock = threading.Lock()

    def _worker(prov: dict[str, Any]):
        p_code = prov["departmentCode"]
        try:
            _, prov_details = fetch_province_agencies_detail_pipeline(
                prov,
                time_type=time_type,
                year=year,
                period=period,
                checkpoint_dir=checkpoint_dir,
                run_date_str=run_date_str,
                timeout=timeout,
                force=force,
            )
            with lock:
                all_agencies_detail.extend(prov_details)
                by_province[p_code] = {
                    "provinceCode": p_code,
                    "provinceName": prov["departmentName"],
                    "rootDepartmentId": prov["rootDepartmentId"],
                    "agencyCount": len(prov_details),
                    "agenciesDetail": prov_details,
                }
            pbar.update(1, status=f"{p_code} ({len(prov_details)} sở)")
        except Exception as exc:
            print(f"\n⚠️ Lỗi khi crawl chi tiết {p_code} ({prov['departmentName']}): {exc}", file=sys.stderr)
            pbar.update(1, status=f"{p_code} Lỗi")

    if concurrency <= 1 or len(provinces) <= 1:
        for prov in provinces:
            _worker(prov)
    else:
        max_workers = min(concurrency, len(provinces))
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(_worker, prov) for prov in provinces]
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as exc:
                    print(f"\n⚠️ Thread error: {exc}", file=sys.stderr)

    pbar.close()

    # Sort all agencies nationwide by totalScore descending
    all_agencies_detail.sort(key=lambda u: (u["totalScore"] is not None, u["totalScore"] or 0), reverse=True)
    for idx, u in enumerate(all_agencies_detail, 1):
        u["rank"] = idx

    return all_agencies_detail, by_province


def update_master_index_detail(
    output_dir: Path,
    details_payload: dict[str, Any],
    run_date_str: str,
) -> Path:
    """Update master index_detail.json database for all agencies nationwide."""
    index_file = output_dir / "index_detail.json"
    index_data = load_json(index_file, {}) or {}

    available_dates = index_data.get("availableDates", [])
    if not isinstance(available_dates, list):
        available_dates = []
    if run_date_str not in available_dates:
        available_dates.append(run_date_str)

    def _date_sort_key(d: str):
        try:
            return datetime.strptime(d, "%d%m%Y")
        except ValueError:
            return datetime.min

    available_dates.sort(key=_date_sort_key)

    agencies_db = index_data.get("agencies", {})
    if not isinstance(agencies_db, dict):
        agencies_db = {}

    agencies_list = details_payload.get("agenciesDetail", [])
    for a in agencies_list:
        did = a.get("departmentId") or a.get("departmentCode")
        if not did:
            continue

        if did not in agencies_db:
            agencies_db[did] = {
                "departmentId": a.get("departmentId"),
                "departmentName": a.get("departmentName"),
                "departmentCode": a.get("departmentCode"),
                "provinceCode": a.get("provinceCode"),
                "provinceName": a.get("provinceName"),
                "rootDepartmentId": a.get("rootDepartmentId"),
                "history": {},
                "latest": {},
            }

        hist_entry = {
            "totalScore": a.get("totalScore"),
            "ratio": a.get("ratio"),
            "scoreDelta": a.get("scoreDelta"),
            "rank": a.get("rank"),
            "groupRank": a.get("groupRank"),
            "groupScores": a.get("groupScores", {}),
            "componentIndicators": a.get("componentIndicators", {}),
        }
        agencies_db[did]["history"][run_date_str] = hist_entry

        if available_dates and available_dates[-1] == run_date_str:
            agencies_db[did]["latest"] = {
                "date": run_date_str,
                "totalScore": a.get("totalScore"),
                "ratio": a.get("ratio"),
                "rank": a.get("rank"),
                "groupRank": a.get("groupRank"),
                "groupScores": a.get("groupScores", {}),
                "componentIndicators": a.get("componentIndicators", {}),
            }

    updated_index = {
        "schemaVersion": 1,
        "updatedAt": utc_now(),
        "latestDate": available_dates[-1] if available_dates else run_date_str,
        "availableDates": available_dates,
        "agencyCount": len(agencies_list),
        "provincesCount": len(details_payload.get("byProvince", {})),
        "agencies": agencies_db,
    }

    write_json(index_file, updated_index)
    return index_file


def clean_old_snapshots(output_dir: Path, clean_days: int) -> int:
    """Clean details snapshot files older than clean_days from data/agencies/."""
    if clean_days <= 0 or not output_dir.exists():
        return 0

    cutoff_date = datetime.now() - timedelta(days=clean_days)
    cutoff_int = int(cutoff_date.strftime("%Y%m%d"))
    deleted_count = 0

    for file_path in output_dir.glob("details_Agencies_*.json"):
        m = re.search(r"_(\d{8})\.json$", file_path.name)
        if m:
            d_str = m.group(1)
            try:
                dt = datetime.strptime(d_str, "%d%m%Y")
                if int(dt.strftime("%Y%m%d")) < cutoff_int:
                    file_path.unlink(missing_ok=True)
                    deleted_count += 1
            except ValueError:
                pass
    return deleted_count


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Crawler & Master Index Detail cho 6 nhóm chỉ số thành phần & sub-metrics tất cả Sở/Ngành 34 tỉnh toàn quốc từ DVCQG."
    )
    parser.add_argument("--time-type", choices=["year", "month", "quarter"], default="year", help="Loại thời gian")
    parser.add_argument("--year", type=int, default=datetime.now().year, help="Năm trích xuất")
    parser.add_argument("--period", type=int, default=None, help="Kỳ trích xuất (tháng 1-12 hoặc quý 1-4)")
    parser.add_argument("--clean-days", type=int, default=3, help="Số ngày dọn dẹp snapshot cũ (mặc định 3)")
    parser.add_argument("--concurrency", type=int, default=3, help="Số luồng đồng thời (mặc định 3)")
    parser.add_argument("--provinces", type=str, default=None, help="Danh sách mã tỉnh lọc, ví dụ: H01,H21 (mặc định tất cả 34 tỉnh)")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR, help="Thư mục xuất dữ liệu")
    parser.add_argument("--raw-dir", type=Path, default=DEFAULT_RAW_DIR, help="Thư mục raw data")
    parser.add_argument("--force", action="store_true", help="Bỏ qua checkpoint, bắt buộc crawl mới")
    parser.add_argument("--timeout", type=int, default=45, help="Timeout kết nối mạng (giây)")
    return parser.parse_args()


def main() -> int:
    args = parse_arguments()
    run_date_str = datetime.now().strftime("%d%m%Y")

    filter_codes = [c.strip() for c in args.provinces.split(",")] if args.provinces else None
    provinces = load_provinces_list(filter_codes)

    print("==============================================================================")
    print("🔬 CRAWLER CHI TIẾT SỞ/NGÀNH TOÀN QUỐC (crawl_agency_detail.py) - DVCQG")
    print(f"📅 Ngày thực thi: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')} (Mã mốc: {run_date_str})")
    print(f"📍 Tổng số tỉnh thực thi: {len(provinces)} tỉnh/thành")
    print(f"⚙️  Thời gian: {args.time_type.upper()} {args.year} (Period: {args.period or 'N/A'})")
    print(f"📁 Thư mục lưu: {args.output_dir}")
    print("==============================================================================")

    if not provinces:
        print("❌ Không tìm thấy danh sách tỉnh để crawl! Vui lòng kiểm tra data/config/provinces.json", file=sys.stderr)
        return 1

    args.output_dir.mkdir(parents=True, exist_ok=True)
    checkpoint_dir = args.raw_dir / str(args.year) / "agencies" / "checkpoints"
    checkpoint_dir.mkdir(parents=True, exist_ok=True)

    start_time = time.time()

    # 1. Crawl all detailed component indicators
    print("\n🚀 [1/3] Đang thu thập 6 nhóm chỉ tiêu thành phần & sub-metrics từ DVCQG...")
    all_agencies_detail, by_province = crawl_all_agencies_detail(
        provinces,
        time_type=args.time_type,
        year=args.year,
        period=args.period,
        checkpoint_dir=checkpoint_dir,
        run_date_str=run_date_str,
        concurrency=args.concurrency,
        timeout=args.timeout,
        force=args.force,
    )

    period_str = f"{args.time_type.capitalize()} {args.period}/{args.year}" if args.period else f"Năm {args.year}"
    details_payload = {
        "metadata": {
            "scope": "Agencies Detail",
            "runDateStr": run_date_str,
            "timeType": args.time_type,
            "year": args.year,
            "period": args.period,
            "periodLabel": period_str,
            "extractedAt": utc_now(),
            "totalProvincesCount": len(by_province),
            "agencyCount": len(all_agencies_detail),
        },
        "count": len(all_agencies_detail),
        "agenciesDetail": all_agencies_detail,
        "byProvince": by_province,
    }

    # 2. Save detailed snapshot
    details_file = args.output_dir / f"details_Agencies_{run_date_str}.json"
    write_json(details_file, details_payload)
    print(f"\n✅ [2/3] Đã lưu Snapshot Chi tiết Sở/Ngành: {details_file} ({len(all_agencies_detail)} sở/ngành)")

    # 3. Update master index_detail.json
    print("\n🔄 [3/3] Cập nhật CSDL Master Detailed Index (index_detail.json)...")
    index_detail_file = update_master_index_detail(args.output_dir, details_payload, run_date_str)
    print(f"✅ Đã cập nhật thành công: {index_detail_file}")

    # 4. Clean old snapshots if clean-days > 0
    if args.clean_days > 0:
        deleted = clean_old_snapshots(args.output_dir, args.clean_days)
        if deleted > 0:
            print(f"🧹 Đã tự động dọn dẹp {deleted} tệp snapshot chi tiết cũ (> {args.clean_days} ngày).")

    elapsed = time.time() - start_time
    print("\n==============================================================================")
    print(f"🎉 HOÀN THÀNH THÀNH CÔNG CRAWLER CHI TIẾT SỞ/NGÀNH TOÀN QUỐC ({elapsed:.1f}s)!")
    print(f"📈 Tổng số Sở/Ngành phân tích chỉ số: {len(all_agencies_detail)} đơn vị trên {len(by_province)} tỉnh/thành.")
    print("==============================================================================")
    return 0


if __name__ == "__main__":
    sys.exit(main())
