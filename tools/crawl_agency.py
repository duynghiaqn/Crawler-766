#!/usr/bin/env python3
"""
Tool crawl_agency.py: Trích xuất, xếp hạng & duy trì CSDL Master Index (index.json) cho tất cả Sở/Ngành (Agencies) của tất cả UBND Tỉnh / Thành phố toàn quốc từ DVCQG.

Endpoint API công khai DVCQG:
1. API Dữ liệu tổng hợp : https://dichvucong.gov.vn/api/v1/reporting/evaluation/service-results
2. API Nhóm chỉ số thành phần (CKMB, TDGQ, ONLINE, TTTT, MDSH, MDHL)

Tính năng chính:
1. Trích xuất đầy đủ điểm số tổng hợp, xếp hạng toàn quốc (National Rank 1..N), xếp hạng nội bộ tỉnh (Province Group Rank 1..M) của tất cả Sở/Ngành (childGroup == 'AGENCY') thuộc 34 UBND Tỉnh/Thành phố.
2. Lưu file CSDL Master Index Database (index.json), snapshot điểm số và file so sánh theo mốc ngày DDMMYYYY trong data/agencies/:
   - index.json                      : API Index Database tổng hợp điểm số, xếp hạng toàn quốc & lịch sử theo ngày cho client lookup.
   - scores_Agencies_DDMMYYYY.json   : Dữ liệu điểm số tổng hợp tất cả Sở/Ngành toàn quốc theo ngày, kèm phân nhóm theo từng tỉnh (byProvince).
   - comparison_Agencies_DDMMYYYY.json : File so sánh điểm số, xếp hạng toàn quốc & xếp hạng theo tỉnh giữa mốc ngày hiện tại với ngày chạy trước đó.
3. Engine So sánh Điểm & Thứ hạng Kỳ Liền (Consecutive Period Comparison): Tự động so sánh chênh lệch điểm số, thứ hạng toàn quốc và thứ hạng trong tỉnh.
4. Tối ưu kết nối & Khôi phục sự cố:
   - Checkpoint Resumption theo từng tỉnh tại data/raw/<year>/agencies/checkpoints/.
   - Cơ chế chống WAF/Firewall: User-Agent Rotation Pool, Header ngẫu nhiên, TLS Session Cookie Warmup & Randomized Sleep Jitter (0.4s - 1.2s).
   - Smart Exponential Backoff Retry cho các lỗi mạng / HTTP 429 / 5xx.
5. Engine Tự động dọn dẹp Snapshot & Index cũ (> N ngày, mặc định: 3 ngày / 72 giờ).

Sử dụng:
  python tools/crawl_agency.py --time-type year --year 2026
  python tools/crawl_agency.py --time-type month --year 2026 --period 3
  python tools/crawl_agency.py --provinces H01,H21
  python tools/crawl_agency.py --clean-days 3
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

# Ensure UTF-8 encoding for console output across all platforms
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

    def __init__(self, total: int, desc: str = "Crawling", unit: str = "tỉnh"):
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
        # Fallback: Try departments.json
        dept_cfg = DATA_DIR / "config" / "departments.json"
        if dept_cfg.exists():
            cfg = load_json(dept_cfg, {})
            provinces = cfg.get("unclassified", [])

    if filter_codes:
        normalized_codes = {c.strip().upper() for c in filter_codes if c.strip()}
        provinces = [p for p in provinces if p.get("departmentCode", "").upper() in normalized_codes]

    # Ensure required fields
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


def fetch_province_agencies_service_results(
    province: dict[str, Any],
    time_type: str,
    year: int,
    period: int | None = None,
    checkpoint_dir: Path | None = None,
    run_date_str: str | None = None,
    timeout: int = 45,
    max_retries: int = 5,
    force: bool = False,
) -> tuple[dict[str, Any], list[dict[str, Any]], dict[str, Any]]:
    """Fetch service-results for a single province and extract all child units with childGroup == 'AGENCY'."""
    p_code = province["departmentCode"]
    p_name = province["departmentName"]
    root_id = province["rootDepartmentId"]

    date_str = run_date_str or datetime.now().strftime("%d%m%Y")
    checkpoint_file = checkpoint_dir / f"checkpoint_agency_{p_code}_{date_str}.json" if checkpoint_dir else None

    detail_checkpoint_file = checkpoint_dir / f"checkpoint_detail_{p_code}_{date_str}.json" if checkpoint_dir else None

    if not force and is_checkpoint_valid(checkpoint_file):
        cached = load_json(checkpoint_file)
        if cached and isinstance(cached, dict) and "agencies" in cached:
            cached_agencies = cached.get("agencies", [])
            if is_checkpoint_valid(detail_checkpoint_file):
                dt_cached = load_json(detail_checkpoint_file)
                if dt_cached and "agenciesDetail" in dt_cached:
                    dt_map = {x["departmentId"]: x.get("groupScores", {}) for x in dt_cached["agenciesDetail"] if x.get("departmentId")}
                    for a in cached_agencies:
                        if not a.get("groupScores") and a["departmentId"] in dt_map:
                            a["groupScores"] = dt_map[a["departmentId"]]
            return province, cached_agencies, cached.get("provinceOverview", {})

    data = fetch_dvc_endpoint(
        ENDPOINT_SERVICE_RESULTS,
        time_type,
        year,
        period,
        root_dept_id=root_id,
        page_size=300,
        current_page=1,
        timeout=timeout,
        max_retries=max_retries,
    )

    overview = data.get("data", {}).get("overview") or {}
    eval_list = data.get("data", {}).get("evaluation") or []
    if not isinstance(eval_list, list):
        eval_list = []

    province_agencies: list[dict[str, Any]] = []
    for item in eval_list:
        if not isinstance(item, dict):
            continue
        if item.get("childGroup") != "AGENCY":
            continue

        did = item.get("departmentId")
        dname = item.get("departmentName")
        dcode = item.get("departmentCode")
        tot_score = item.get("totalScore")
        ratio = item.get("ratio")
        delta = item.get("scoreDelta")

        agency_record = {
            "departmentId": did,
            "departmentName": dname,
            "departmentCode": dcode,
            "childGroup": "AGENCY",
            "provinceCode": p_code,
            "provinceName": p_name,
            "rootDepartmentId": root_id,
            "totalScore": round(float(tot_score), 2) if tot_score is not None else None,
            "totalMaxScore": 100,
            "ratio": round(float(ratio), 2) if ratio is not None else None,
            "scoreDelta": round(float(delta), 2) if delta is not None else None,
            "groupScores": item.get("groupScores", {}),
        }
        province_agencies.append(agency_record)

    # Sort agencies within this province descending
    province_agencies.sort(
        key=lambda a: (a["totalScore"] is not None, a["totalScore"] or 0),
        reverse=True,
    )
    for idx, a in enumerate(province_agencies, 1):
        a["groupRank"] = idx

    prov_overview = {
        "departmentName": overview.get("departmentName", p_name),
        "departmentCode": p_code,
        "totalScore": round(float(overview.get("totalScore", 0)), 2) if overview.get("totalScore") is not None else None,
        "ratio": round(float(overview.get("ratio", 0)), 2) if overview.get("ratio") is not None else None,
        "agencyCount": len(province_agencies),
    }

    # Try to enrich groupScores from detail checkpoint if available
    detail_checkpoint_file = checkpoint_dir / f"checkpoint_detail_{p_code}_{date_str}.json" if checkpoint_dir else None
    if is_checkpoint_valid(detail_checkpoint_file):
        dt_cached = load_json(detail_checkpoint_file)
        if dt_cached and "agenciesDetail" in dt_cached:
            dt_map = {x["departmentId"]: x.get("groupScores", {}) for x in dt_cached["agenciesDetail"] if x.get("departmentId")}
            for a in province_agencies:
                if a["departmentId"] in dt_map and dt_map[a["departmentId"]]:
                    a["groupScores"] = dt_map[a["departmentId"]]

    if checkpoint_file:
        write_json(checkpoint_file, {
            "provinceCode": p_code,
            "provinceName": p_name,
            "provinceOverview": prov_overview,
            "agencies": province_agencies,
            "updatedAt": utc_now(),
        })

    return province, province_agencies, prov_overview


def crawl_all_agencies(
    provinces: list[dict[str, Any]],
    time_type: str,
    year: int,
    period: int | None = None,
    checkpoint_dir: Path | None = None,
    run_date_str: str | None = None,
    concurrency: int = 4,
    timeout: int = 45,
    force: bool = False,
) -> tuple[list[dict[str, Any]], dict[str, Any], dict[str, dict[str, Any]]]:
    """Crawl service results for all provinces and assemble complete nationwide agencies dataset."""
    all_agencies: list[dict[str, Any]] = []
    by_province: dict[str, dict[str, Any]] = {}
    pbar = CrawlerProgressBar(total=len(provinces), desc="Crawling Agencies", unit="tỉnh")

    warmup_session(timeout=timeout)

    lock = threading.Lock()

    def _worker(prov: dict[str, Any]):
        p_code = prov["departmentCode"]
        try:
            _, prov_agencies, prov_ov = fetch_province_agencies_service_results(
                prov,
                time_type,
                year,
                period,
                checkpoint_dir=checkpoint_dir,
                run_date_str=run_date_str,
                timeout=timeout,
                force=force,
            )
            with lock:
                all_agencies.extend(prov_agencies)
                by_province[p_code] = {
                    "provinceCode": p_code,
                    "provinceName": prov["departmentName"],
                    "rootDepartmentId": prov["rootDepartmentId"],
                    "overview": prov_ov,
                    "agencyCount": len(prov_agencies),
                    "agencies": prov_agencies,
                }
            pbar.update(1, status=f"{p_code} ({len(prov_agencies)} sở)")
        except Exception as exc:
            print(f"\n⚠️ Lỗi khi crawl {p_code} ({prov['departmentName']}): {exc}", file=sys.stderr)
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
                    print(f"\n⚠️ Thread execution error: {exc}", file=sys.stderr)

    pbar.close()

    # Sort ALL agencies nationwide by totalScore descending
    all_agencies.sort(
        key=lambda a: (a["totalScore"] is not None, a["totalScore"] or 0),
        reverse=True,
    )

    # Assign nationwide rank 1..N
    for idx, a in enumerate(all_agencies, 1):
        a["rank"] = idx

    # Compute national agencies summary metrics
    valid_scores = [a["totalScore"] for a in all_agencies if a["totalScore"] is not None]
    avg_score = round(sum(valid_scores) / len(valid_scores), 2) if valid_scores else 0.0
    max_score = max(valid_scores) if valid_scores else 0.0
    min_score = min(valid_scores) if valid_scores else 0.0

    overview = {
        "scope": "All Provincial Agencies (Khối Sở / Ban / Ngành toàn quốc)",
        "totalAgencies": len(all_agencies),
        "totalProvincesCount": len(by_province),
        "averageScore": avg_score,
        "maxScore": max_score,
        "minScore": min_score,
        "topAgency": all_agencies[0] if all_agencies else None,
    }

    return all_agencies, overview, by_province


def find_previous_daily_date_from_index(
    index_data: dict[str, Any],
    current_date_str: str,
    explicit_compare_date: str | None = None,
) -> str | None:
    """Find the previous daily date string (DDMMYYYY) from index_data['availableDates']."""
    if explicit_compare_date:
        return explicit_compare_date

    avail = index_data.get("availableDates", [])
    if isinstance(avail, list) and len(avail) > 0:
        valid_dates = []
        for d in avail:
            try:
                dt = datetime.strptime(d, "%d%m%Y")
                valid_dates.append((dt, d))
            except ValueError:
                pass
        valid_dates.sort(key=lambda x: x[0])
        sorted_strs = [d for _, d in valid_dates]

        try:
            curr_dt = datetime.strptime(current_date_str, "%d%m%Y")
            past = [d for dt, d in valid_dates if dt < curr_dt]
            if past:
                return past[-1]
        except ValueError:
            pass

        if current_date_str in sorted_strs:
            idx = sorted_strs.index(current_date_str)
            if idx > 0:
                return sorted_strs[idx - 1]

    return None


def build_agencies_comparison(
    curr_score_data: dict[str, Any],
    prev_score_data: dict[str, Any] | None,
    curr_date_str: str,
    prev_date_str: str | None,
) -> dict[str, Any]:
    """Generate comprehensive daily comparison of scores and ranks for all agencies."""
    curr_agencies = curr_score_data.get("agencies", [])
    prev_agencies_map = {}
    if prev_score_data:
        for a in prev_score_data.get("agencies", []):
            did = a.get("departmentId") or a.get("departmentCode")
            if did:
                prev_agencies_map[did] = a

    comparison_list: list[dict[str, Any]] = []
    for a in curr_agencies:
        did = a.get("departmentId") or a.get("departmentCode")
        prev_a = prev_agencies_map.get(did)

        curr_score = a.get("totalScore")
        curr_rank = a.get("rank")
        curr_grank = a.get("groupRank")

        score_diff = None
        rank_diff = None
        group_rank_diff = None

        if prev_a:
            prev_score = prev_a.get("totalScore")
            if curr_score is not None and prev_score is not None:
                score_diff = round(float(curr_score) - float(prev_score), 2)

            prev_rank = prev_a.get("rank")
            if curr_rank is not None and prev_rank is not None:
                rank_diff = prev_rank - curr_rank  # Positive = improved rank

            prev_grank = prev_a.get("groupRank")
            if curr_grank is not None and prev_grank is not None:
                group_rank_diff = prev_grank - curr_grank

        rec = {
            "departmentId": a.get("departmentId"),
            "departmentName": a.get("departmentName"),
            "departmentCode": a.get("departmentCode"),
            "provinceCode": a.get("provinceCode"),
            "provinceName": a.get("provinceName"),
            "current": {
                "totalScore": curr_score,
                "rank": curr_rank,
                "groupRank": curr_grank,
                "ratio": a.get("ratio"),
            },
            "previous": {
                "date": prev_date_str,
                "totalScore": prev_a.get("totalScore") if prev_a else None,
                "rank": prev_a.get("rank") if prev_a else None,
                "groupRank": prev_a.get("groupRank") if prev_a else None,
                "ratio": prev_a.get("ratio") if prev_a else None,
            } if prev_a else None,
            "comparison": {
                "scoreDiff": score_diff,
                "rankDiff": rank_diff,
                "groupRankDiff": group_rank_diff,
                "trend": "UP" if (score_diff and score_diff > 0) else ("DOWN" if (score_diff and score_diff < 0) else "UNCHANGED"),
            },
        }
        comparison_list.append(rec)

    # Sort comparison by current rank
    comparison_list.sort(key=lambda x: (x["current"]["rank"] is not None, x["current"]["rank"] or 9999))

    return {
        "metadata": {
            "scope": "Agencies Comparison",
            "currentDate": curr_date_str,
            "previousDate": prev_date_str,
            "totalAgencies": len(comparison_list),
            "generatedAt": utc_now(),
        },
        "agenciesComparison": comparison_list,
    }


def update_master_index(
    output_dir: Path,
    score_data: dict[str, Any],
    run_date_str: str,
) -> Path:
    """Update master index.json database for all agencies nationwide."""
    index_file = output_dir / "index.json"
    index_data = load_json(index_file, {}) or {}

    available_dates = index_data.get("availableDates", [])
    if not isinstance(available_dates, list):
        available_dates = []
    if run_date_str not in available_dates:
        available_dates.append(run_date_str)

    # Sort available dates chronologically
    def _date_sort_key(d: str):
        try:
            return datetime.strptime(d, "%d%m%Y")
        except ValueError:
            return datetime.min

    available_dates.sort(key=_date_sort_key)

    agencies_db = index_data.get("agencies", {})
    if not isinstance(agencies_db, dict):
        agencies_db = {}

    agencies_list = score_data.get("agencies", [])
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
        }
        agencies_db[did]["history"][run_date_str] = hist_entry

        # Update latest if run_date_str is newest date
        if available_dates and available_dates[-1] == run_date_str:
            agencies_db[did]["latest"] = {
                "date": run_date_str,
                "totalScore": a.get("totalScore"),
                "ratio": a.get("ratio"),
                "rank": a.get("rank"),
                "groupRank": a.get("groupRank"),
                "groupScores": a.get("groupScores", {}),
            }

    # Grouping by province in master index
    provinces_index = {}
    for p_code, p_info in score_data.get("byProvince", {}).items():
        provinces_index[p_code] = {
            "provinceCode": p_code,
            "provinceName": p_info.get("provinceName"),
            "agencyCount": p_info.get("agencyCount"),
            "agencyIds": [ag.get("departmentId") for ag in p_info.get("agencies", []) if ag.get("departmentId")],
        }

    updated_index = {
        "schemaVersion": 1,
        "updatedAt": utc_now(),
        "latestDate": available_dates[-1] if available_dates else run_date_str,
        "availableDates": available_dates,
        "overview": score_data.get("overview", {}),
        "provincesIndex": provinces_index,
        "agencies": agencies_db,
    }

    write_json(index_file, updated_index)
    return index_file


def clean_old_snapshots(output_dir: Path, clean_days: int) -> int:
    """Clean snapshot files older than clean_days from data/agencies/."""
    if clean_days <= 0 or not output_dir.exists():
        return 0

    cutoff_date = datetime.now() - timedelta(days=clean_days)
    cutoff_int = int(cutoff_date.strftime("%Y%m%d"))
    deleted_count = 0

    for file_path in output_dir.glob("*.json"):
        if file_path.name in ("index.json", "index_detail.json"):
            continue
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
        description="Crawler & Master Indexer cho tất cả Sở/Ngành (Agencies) của 34 tỉnh/thành phố toàn quốc từ DVCQG."
    )
    parser.add_argument("--time-type", choices=["year", "month", "quarter"], default="year", help="Loại thời gian")
    parser.add_argument("--year", type=int, default=datetime.now().year, help="Năm trích xuất")
    parser.add_argument("--period", type=int, default=None, help="Kỳ trích xuất (tháng 1-12 hoặc quý 1-4)")
    parser.add_argument("--clean-days", type=int, default=3, help="Số ngày dọn dẹp snapshot cũ (mặc định 3)")
    parser.add_argument("--compare-date", type=str, default=None, help="Mốc ngày so sánh (DDMMYYYY)")
    parser.add_argument("--concurrency", type=int, default=4, help="Số luồng đồng thời (mặc định 4)")
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
    print("🏛️  CRAWLER SỞ/NGÀNH TOÀN QUỐC (crawl_agency.py) - DVCQG")
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

    # 1. Crawl all agencies
    print("\n🚀 [1/4] Đang thu thập điểm số các Sở/Ngành từ DVCQG...")
    all_agencies, overview, by_province = crawl_all_agencies(
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
    scores_payload = {
        "metadata": {
            "scope": "Agencies",
            "runDateStr": run_date_str,
            "timeType": args.time_type,
            "year": args.year,
            "period": args.period,
            "periodLabel": period_str,
            "extractedAt": utc_now(),
            "totalProvincesCount": len(by_province),
            "agencyCount": len(all_agencies),
            "sourceUrl": ENDPOINT_SERVICE_RESULTS,
        },
        "overview": overview,
        "agencies": all_agencies,
        "byProvince": by_province,
    }

    # 2. Save scores snapshot
    scores_file = args.output_dir / f"scores_Agencies_{run_date_str}.json"
    write_json(scores_file, scores_payload)
    print(f"\n✅ [2/4] Đã lưu Snapshot Điểm số Sở/Ngành: {scores_file} ({len(all_agencies)} sở/ngành)")

    # 3. Update master index.json
    index_file = args.output_dir / "index.json"
    prev_index_data = load_json(index_file, {}) or {}
    prev_date_str = find_previous_daily_date_from_index(prev_index_data, run_date_str, args.compare_date)

    print("\n🔄 [3/4] Cập nhật CSDL Master Index (index.json)...")
    update_master_index(args.output_dir, scores_payload, run_date_str)
    print(f"✅ Đã cập nhật thành công: {index_file}")

    # 4. Generate comparison file
    print(f"\n📊 [4/4] Tạo file So sánh Điểm & Xếp hạng (Kỳ trước: {prev_date_str or 'Lần đầu tiên chạy'})...")
    prev_score_data = None
    if prev_date_str:
        prev_score_file = args.output_dir / f"scores_Agencies_{prev_date_str}.json"
        prev_score_data = load_json(prev_score_file)

    comp_payload = build_agencies_comparison(scores_payload, prev_score_data, run_date_str, prev_date_str)
    comp_file = args.output_dir / f"comparison_Agencies_{run_date_str}.json"
    write_json(comp_file, comp_payload)
    print(f"✅ Đã lưu File So sánh: {comp_file}")

    # 5. Clean old snapshots if clean-days > 0
    if args.clean_days > 0:
        deleted = clean_old_snapshots(args.output_dir, args.clean_days)
        if deleted > 0:
            print(f"🧹 Đã tự động dọn dẹp {deleted} tệp snapshot cũ (> {args.clean_days} ngày).")

    elapsed = time.time() - start_time
    print("\n==============================================================================")
    print(f"🎉 HOÀN THÀNH THÀNH CÔNG CRAWLER SỞ/NGÀNH TOÀN QUỐC ({elapsed:.1f}s)!")
    print(f"📈 Tổng số Sở/Ngành thu thập: {len(all_agencies)} đơn vị trên {len(by_province)} tỉnh/thành.")
    if all_agencies:
        top1 = all_agencies[0]
        print(f"🏆 Quán quân Toàn quốc: {top1['departmentName']} ({top1['provinceName']}) - Điểm: {top1['totalScore']}")
    print("==============================================================================")
    return 0


if __name__ == "__main__":
    sys.exit(main())
