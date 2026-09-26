#!/usr/bin/env python3
"""
Tool crawl_gl_detail.py: Trích xuất Dữ liệu Chi tiết Tất cả Chỉ tiêu Thành phần các Đơn vị con UBND Tỉnh Gia Lai.

Endpoint API công khai DVCQG:
1. API Dữ liệu tổng hợp : https://dichvucong.gov.vn/api/v1/reporting/evaluation/service-results
2. API Công khai minh bạch : https://dichvucong.gov.vn/api/v1/reporting/evaluation/transparency (hoặc /formalities)
3. API Mức độ hài lòng    : https://dichvucong.gov.vn/api/v1/reporting/evaluation/handling-satisfaction
4. API Số hóa hồ sơ       : https://dichvucong.gov.vn/api/v1/reporting/evaluation/dossier-digitized
5. API Tiến độ giải quyết  : https://dichvucong.gov.vn/api/v1/reporting/evaluation/dvc-progress-tree
6. API Dịch vụ công trực tuyến : https://dichvucong.gov.vn/api/v1/reporting/evaluation/provide-online-tree
7. API Thanh toán trực tuyến   : https://dichvucong.gov.vn/api/v1/reporting/evaluation/formality-online-payment-tree

Tính năng cao cấp:
1. Trích xuất chi tiết từng chỉ số thành phần (metrics, số hồ sơ đúng hạn/quá hạn/tiếp nhận/số hóa/thanh toán) của tất cả 149 đơn vị con (Sở/Ngành & Xã/Phường).
2. Xoay vòng User-Agent Header & Randomized Sleep Jitter chống WAF rate-limit.
3. Tự động thử lại thông minh với Smart Exponential Backoff Retry.
4. Cơ chế Checkpoint Resumption theo điểm ngắt lỗi đĩa (checkpoint_detail_DDMMYYYY.json).
5. Xuất các file JSON báo cáo chi tiết: details_GiaLai_DDMMYYYY.json, agencies_detail_..., communes_detail_... và cập nhật index.json.

Sử dụng:
  python tools/crawl_gl_detail.py --time-type year --year 2026
  python tools/crawl_gl_detail.py --time-type month --year 2026 --period 3
"""

from __future__ import annotations

import argparse
import json
import random
import re
import sys
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

# Default Configuration Constants
ENDPOINT_SERVICE_RESULTS = "https://dichvucong.gov.vn/api/v1/reporting/evaluation/service-results"
ENDPOINT_FORMALITIES = "https://dichvucong.gov.vn/api/v1/reporting/formalities"
ENDPOINT_TRANSPARENCY = "https://dichvucong.gov.vn/api/v1/reporting/evaluation/transparency"
ENDPOINT_DIGITIZED = "https://dichvucong.gov.vn/api/v1/reporting/evaluation/dossier-digitized"
ENDPOINT_PROGRESS = "https://dichvucong.gov.vn/api/v1/reporting/evaluation/dvc-progress-tree"
ENDPOINT_ONLINE = "https://dichvucong.gov.vn/api/v1/reporting/evaluation/provide-online-tree"
ENDPOINT_PAYMENT = "https://dichvucong.gov.vn/api/v1/reporting/evaluation/formality-online-payment-tree"
ENDPOINT_HANDLING_SATISFACTION = "https://dichvucong.gov.vn/api/v1/reporting/evaluation/handling-satisfaction"

GIA_LAI_ROOT_ID = "019d2be3-6a85-74ec-a346-6489e82ae4c7"
GIA_LAI_CODE = "H21"
GIA_LAI_NAME = "UBND tỉnh Gia Lai"

ROOT_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT_DIR / "data"
DEFAULT_OUTPUT_DIR = DATA_DIR / "gia_lai"
DEFAULT_RAW_DIR = DATA_DIR / "raw"

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4.1 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
]

try:
    from tqdm import tqdm
    HAS_TQDM = True
except ImportError:
    HAS_TQDM = False


class CrawlerProgressBar:
    """Flexible thread-safe progress bar supporting tqdm with fallback to clean standard console output."""

    def __init__(self, total: int, desc: str = "Crawling Details", unit: str = "endpoint"):
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
        if 0 < self.current < self.total:
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
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def load_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def get_random_headers() -> dict[str, str]:
    user_agent = random.choice(USER_AGENTS)
    headers = {
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json",
        "User-Agent": user_agent,
        "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
        "Referer": "https://dichvucong.gov.vn/danh-gia-chat-luong-phuc-vu",
        "Origin": "https://dichvucong.gov.vn",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
    }
    if "Chrome" in user_agent:
        headers["Sec-Ch-Ua"] = '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"'
        headers["Sec-Ch-Ua-Mobile"] = "?0"
        headers["Sec-Ch-Ua-Platform"] = '"Windows"' if "Windows" in user_agent else '"macOS"'
    return headers


def fetch_dvc_endpoint(
    url: str,
    time_type: str,
    year: int,
    period: int | None = None,
    root_dept_id: str | None = GIA_LAI_ROOT_ID,
    page_size: int = 300,
    timeout: int = 45,
    max_retries: int = 6,
    delay_min: float = 1.0,
    delay_max: float = 2.5,
) -> dict[str, Any]:
    """Fetch DVCQG API endpoint with header rotation, random delay jitter, longer timeout, and smart exponential backoff retries."""
    payload: dict[str, Any] = {
        "timeType": time_type,
        "year": year,
        "pageSize": page_size,
        "currentPage": 1,
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

    sleep_sec = random.uniform(delay_min, delay_max) + random.uniform(0.05, 0.25)
    time.sleep(sleep_sec)

    for attempt in range(1, max_retries + 1):
        headers = get_random_headers()
        request = urllib.request.Request(url, data=body, method="POST", headers=headers)
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                status = response.status
                raw_text = response.read().decode("utf-8", errors="replace")
                parsed = json.loads(raw_text)
                if status not in (200, 201):
                    raise RuntimeError(f"API returned HTTP status {status}")
                return parsed
        except urllib.error.HTTPError as exc:
            err_body = exc.read().decode("utf-8", errors="replace")
            if exc.code in (429, 403, 500, 502, 503, 504) and attempt < max_retries:
                retry_wait = (3.0 * (1.4 ** (attempt - 1))) + random.uniform(1.0, 3.5)
                print(f"\n⚠️ [Thử lại {attempt}/{max_retries}] HTTP {exc.code} từ DVCQG. Đợi {retry_wait:.1f}s...", file=sys.stderr)
                time.sleep(retry_wait)
                continue
            raise RuntimeError(f"HTTP Error {exc.code}: {err_body[:500]}") from exc
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            reason = getattr(exc, "reason", str(exc))
            if attempt < max_retries:
                retry_wait = (3.0 * (1.4 ** (attempt - 1))) + random.uniform(1.0, 3.5)
                print(f"\n⚠️ [Thử lại {attempt}/{max_retries}] Lỗi kết nối DVCQG ({reason}). Tự động thử lại sau {retry_wait:.1f}s...", file=sys.stderr)
                time.sleep(retry_wait)
                continue
            raise RuntimeError(f"Network error connecting to DVCQG: {reason}") from exc

    raise RuntimeError(f"Failed to fetch DVCQG endpoint after {max_retries} attempts")


def _fetch_single_detail_endpoint(
    code: str,
    url: str,
    data_keys: list[str],
    time_type: str,
    year: int,
    period: int | None,
    timeout: int,
    max_retries: int,
    delay_min: float,
    delay_max: float,
    pbar: CrawlerProgressBar | None = None,
) -> tuple[str, dict[str, Any]]:
    """Worker task to fetch a single detailed criteria endpoint."""
    raw_response = {}
    try:
        if pbar:
            pbar.set_postfix_str(f"Fetching {code}...")
        raw_response = fetch_dvc_endpoint(
            url,
            time_type,
            year,
            period,
            timeout=timeout,
            max_retries=max_retries,
            delay_min=delay_min,
            delay_max=delay_max,
        )
        if pbar:
            pbar.update(1, status=f"{code} OK")
    except Exception as exc:
        print(f"\n⚠️  Warning fetching {code} detailed criteria: {exc}", file=sys.stderr)
        if pbar:
            pbar.update(1, status=f"{code} Warn")

    return code, raw_response


def fetch_all_detailed_endpoints(
    time_type: str,
    year: int,
    period: int | None,
    checkpoint_file: Path | None = None,
    timeout: int = 45,
    max_retries: int = 6,
    delay_min: float = 1.0,
    delay_max: float = 2.5,
    concurrency: int = 5,
    pbar: CrawlerProgressBar | None = None,
) -> dict[str, dict[str, Any]]:
    """Fetch all 7 detailed criteria API endpoints for Gia Lai with multi-threaded partition & checkpoint resumption."""
    raw_endpoints: dict[str, dict[str, Any]] = {
        "SERVICE_RESULTS": {},
        "TRANSPARENCY": {},
        "DIGITIZED": {},
        "PROGRESS": {},
        "ONLINE": {},
        "PAYMENT": {},
        "HANDLING_SATISFACTION": {},
    }

    if checkpoint_file and checkpoint_file.exists():
        cached = load_json(checkpoint_file)
        if isinstance(cached, dict):
            for k in raw_endpoints:
                if k in cached and isinstance(cached[k], dict) and len(cached[k]) > 0:
                    raw_endpoints[k] = cached[k]

    tasks = [
        ("SERVICE_RESULTS", ENDPOINT_SERVICE_RESULTS, ["evaluation"]),
        ("TRANSPARENCY", ENDPOINT_TRANSPARENCY, ["evaluation"]),
        ("DIGITIZED", ENDPOINT_DIGITIZED, ["evaluation"]),
        ("PROGRESS", ENDPOINT_PROGRESS, ["children"]),
        ("ONLINE", ENDPOINT_ONLINE, ["children"]),
        ("PAYMENT", ENDPOINT_PAYMENT, ["children"]),
        ("HANDLING_SATISFACTION", ENDPOINT_HANDLING_SATISFACTION, ["evaluation"]),
    ]

    pending_tasks = [t for t in tasks if not raw_endpoints[t[0]]]
    completed_count = len(tasks) - len(pending_tasks)

    if completed_count > 0:
        if pbar:
            pbar.update(completed_count, status=f"Checkpoint {completed_count}/{len(tasks)} OK")
        print(f"  ℹ️ Đã tự động khôi phục {completed_count}/{len(tasks)} API endpoints từ mốc checkpoint đĩa (bỏ qua fetch lại).")

    if not pending_tasks:
        return raw_endpoints

    lock = threading.Lock()

    def _worker(code: str, url: str, d_keys: list[str]) -> tuple[str, dict[str, Any]]:
        ep_code, ep_raw = _fetch_single_detail_endpoint(
            code, url, d_keys, time_type, year, period, timeout, max_retries, delay_min, delay_max, pbar
        )
        with lock:
            if ep_raw:
                raw_endpoints[ep_code] = ep_raw
                if checkpoint_file:
                    write_json(checkpoint_file, raw_endpoints)
        return ep_code, ep_raw

    if concurrency <= 1 or len(pending_tasks) == 1:
        for code, url, d_keys in pending_tasks:
            _worker(code, url, d_keys)
    else:
        max_workers = min(concurrency, len(pending_tasks))
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(_worker, c, u, dk) for c, u, dk in pending_tasks]
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as exc:
                    print(f"\n⚠️  Thread partition error: {exc}", file=sys.stderr)

    return raw_endpoints


def process_detailed_units(raw_endpoints: dict[str, dict[str, Any]]) -> dict[str, Any]:
    """Combine and build complete sub-criteria indicator breakdown for all 149 Gia Lai child units."""
    sr_raw = raw_endpoints.get("SERVICE_RESULTS", {}).get("data", {})
    trans_raw = raw_endpoints.get("TRANSPARENCY", {}).get("data", {})
    digi_raw = raw_endpoints.get("DIGITIZED", {}).get("data", {})
    prog_raw = raw_endpoints.get("PROGRESS", {}).get("data", {})
    online_raw = raw_endpoints.get("ONLINE", {}).get("data", {})
    pay_raw = raw_endpoints.get("PAYMENT", {}).get("data", {})
    sat_raw = raw_endpoints.get("HANDLING_SATISFACTION", {}).get("data", {})

    overview = sr_raw.get("overview") or {}
    evaluation_list = sr_raw.get("evaluation") or []

    # Map departmentId to item dictionary for each API
    trans_map = {item["departmentId"]: item for item in trans_raw.get("evaluation", []) if isinstance(item, dict) and item.get("departmentId")}
    digi_map = {item["departmentId"]: item for item in digi_raw.get("evaluation", []) if isinstance(item, dict) and item.get("departmentId")}
    prog_map = {item["departmentId"]: item for item in prog_raw.get("children", []) if isinstance(item, dict) and item.get("departmentId")}
    online_map = {item["departmentId"]: item for item in online_raw.get("children", []) if isinstance(item, dict) and item.get("departmentId")}
    pay_map = {item["departmentId"]: item for item in pay_raw.get("children", []) if isinstance(item, dict) and item.get("departmentId")}
    sat_map = {item["departmentId"]: item for item in sat_raw.get("evaluation", []) if isinstance(item, dict) and item.get("departmentId")}

    units_detail: list[dict[str, Any]] = []
    agencies_detail: list[dict[str, Any]] = []
    communes_detail: list[dict[str, Any]] = []

    for item in evaluation_list:
        if not isinstance(item, dict):
            continue
        did = item.get("departmentId")
        if not did:
            continue

        name = item.get("departmentName")
        code = item.get("departmentCode")
        child_group = item.get("childGroup")
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

        unit_record = {
            "departmentId": did,
            "departmentName": name,
            "departmentCode": code,
            "childGroup": child_group,
            "totalScore": total_score,
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
                }
            }
        }

        units_detail.append(unit_record)
        if child_group == "AGENCY":
            agencies_detail.append(unit_record)
        elif child_group == "COMMUNE":
            communes_detail.append(unit_record)

    units_detail.sort(key=lambda u: u["totalScore"], reverse=True)
    agencies_detail.sort(key=lambda u: u["totalScore"], reverse=True)
    communes_detail.sort(key=lambda u: u["totalScore"], reverse=True)

    for idx, u in enumerate(units_detail, 1):
        u["rank"] = idx

    for idx, u in enumerate(agencies_detail, 1):
        u["groupRank"] = idx

    for idx, u in enumerate(communes_detail, 1):
        u["groupRank"] = idx

    return {
        "overview": overview,
        "units": units_detail,
        "agencies": agencies_detail,
        "communes": communes_detail,
    }


def update_api_index_detail(
    output_dir: Path,
    processed: dict[str, Any],
    metadata: dict[str, Any],
    date_str: str,
) -> tuple[Path, Path]:
    """
    Build & update both master index files:
    1. index.json (Primary root index - updated with latest detailed indicators)
    2. index_detail.json (Master detailed lookup database index)
    """
    index_path = output_dir / "index.json"
    index_detail_path = output_dir / "index_detail.json"

    # --- 1. Update master index.json ---
    index_data = load_json(index_path) or {}
    if not isinstance(index_data, dict):
        index_data = {}

    index_data["schemaVersion"] = 1
    index_data["updatedAt"] = utc_now()

    province_info = index_data.setdefault("province", {})
    province_info["name"] = GIA_LAI_NAME
    province_info["code"] = GIA_LAI_CODE
    province_info["id"] = GIA_LAI_ROOT_ID

    avail_dates = set(index_data.get("availableDates") or [])
    avail_dates.add(date_str)
    sorted_avail = sorted(list(avail_dates))
    index_data["availableDates"] = sorted_avail

    latest_date = date_str
    dates_before = [d for d in sorted_avail if d < latest_date]
    prev_date = dates_before[-1] if dates_before else None

    index_data["latestDate"] = latest_date
    index_data["previousDate"] = prev_date

    ov_hist = index_data.setdefault("overviewHistory", {})
    ov_hist[date_str] = {
        "date": date_str,
        "periodLabel": metadata["periodLabel"],
        "totalScore": processed["overview"].get("totalScore"),
        "ratio": processed["overview"].get("ratio"),
        "scoreDelta": processed["overview"].get("scoreDelta"),
        "groupScores": processed["overview"].get("groupScores"),
        "totalUnitsCount": len(processed["units"]),
        "agencyCount": len(processed["agencies"]),
        "communeCount": len(processed["communes"]),
    }

    index_data["latestOverview"] = ov_hist.get(latest_date)
    index_data["previousOverview"] = ov_hist.get(prev_date) if prev_date else None

    dept_index = index_data.setdefault("departmentsIndex", {})
    for unit in processed.get("units", []):
        code = unit.get("departmentCode")
        did = unit.get("departmentId")
        if not code or not did:
            continue

        d_entry = dept_index.setdefault(code, {
            "departmentId": did,
            "departmentName": unit["departmentName"],
            "departmentCode": code,
            "childGroup": unit["childGroup"],
            "latest": {},
            "previous": None,
            "history": {},
        })

        unit_record = {
            "date": date_str,
            "periodLabel": metadata["periodLabel"],
            "totalScore": unit.get("totalScore"),
            "ratio": unit.get("ratio"),
            "scoreDelta": unit.get("scoreDelta"),
            "rank": unit.get("rank"),
            "groupRank": unit.get("groupRank"),
            "groupScores": unit.get("groupScores"),
            "componentIndicators": unit.get("componentIndicators"),
        }

        d_entry.setdefault("history", {})[date_str] = unit_record

    for code, d_entry in dept_index.items():
        hist = d_entry.get("history", {})
        d_entry["latest"] = hist.get(latest_date)
        d_entry["previous"] = hist.get(prev_date) if prev_date else None

    write_json(index_path, index_data)

    # --- 2. Update master index_detail.json ---
    detail_index_data = load_json(index_detail_path) or {}
    if not isinstance(detail_index_data, dict):
        detail_index_data = {}

    detail_index_data["schemaVersion"] = 1
    detail_index_data["updatedAt"] = utc_now()
    detail_index_data["latestDate"] = latest_date
    detail_index_data["previousDate"] = prev_date
    detail_index_data["province"] = province_info
    detail_index_data["availableDates"] = sorted_avail
    detail_index_data["overviewHistory"] = ov_hist
    detail_index_data["latestOverview"] = ov_hist.get(latest_date)
    detail_index_data["previousOverview"] = ov_hist.get(prev_date) if prev_date else None

    detail_index_data["agenciesList"] = [
        {
            "code": a["departmentCode"],
            "id": a["departmentId"],
            "name": a["departmentName"],
            "rank": a.get("groupRank"),
            "score": a.get("totalScore"),
            "groupScores": a.get("groupScores"),
            "componentIndicators": a.get("componentIndicators"),
        }
        for a in processed.get("agencies", [])
    ]

    detail_index_data["communesList"] = [
        {
            "code": c["departmentCode"],
            "id": c["departmentId"],
            "name": c["departmentName"],
            "rank": c.get("groupRank"),
            "score": c.get("totalScore"),
            "groupScores": c.get("groupScores"),
            "componentIndicators": c.get("componentIndicators"),
        }
        for c in processed.get("communes", [])
    ]

    dept_detail_idx = detail_index_data.setdefault("departmentsDetailIndex", {})
    for unit in processed.get("units", []):
        code = unit.get("departmentCode")
        did = unit.get("departmentId")
        if not code or not did:
            continue

        d_entry = dept_detail_idx.setdefault(code, {
            "departmentId": did,
            "departmentName": unit["departmentName"],
            "departmentCode": code,
            "childGroup": unit["childGroup"],
            "latest": {},
            "previous": None,
            "history": {},
        })

        unit_detail_record = {
            "date": date_str,
            "periodLabel": metadata["periodLabel"],
            "totalScore": unit.get("totalScore"),
            "ratio": unit.get("ratio"),
            "scoreDelta": unit.get("scoreDelta"),
            "rank": unit.get("rank"),
            "groupRank": unit.get("groupRank"),
            "groupScores": unit.get("groupScores"),
            "componentIndicators": unit.get("componentIndicators"),
        }

        d_entry.setdefault("history", {})[date_str] = unit_detail_record

    for code, d_entry in dept_detail_idx.items():
        hist = d_entry.get("history", {})
        d_entry["latest"] = hist.get(latest_date)
        d_entry["previous"] = hist.get(prev_date) if prev_date else None

    write_json(index_detail_path, detail_index_data)

    return index_path, index_detail_path


def clean_old_snapshots(output_dir: Path, raw_dir: Path, retention_days: int = 3) -> int:
    cutoff_date = datetime.now() - timedelta(days=retention_days)
    deleted_files = 0

    patterns = [
        (output_dir, "details_GiaLai_*.json"),
        (output_dir, "agencies_detail_GiaLai_*.json"),
        (output_dir, "communes_detail_GiaLai_*.json"),
        (raw_dir / "2026" / "gia_lai", "raw_GiaLai_detail_*.json"),
        (raw_dir / "2026" / "gia_lai" / "checkpoints", "checkpoint_detail_*.json"),
    ]

    for parent_dir, pattern in patterns:
        if not parent_dir.exists():
            continue
        for fpath in parent_dir.glob(pattern):
            match = re.search(r"_(\d{8})\.json$", fpath.name)
            if not match:
                continue
            d_str = match.group(1)
            try:
                file_dt = datetime.strptime(d_str, "%d%m%Y")
                if file_dt < cutoff_date.replace(hour=0, minute=0, second=0, microsecond=0):
                    fpath.unlink()
                    deleted_files += 1
            except ValueError:
                pass

    return deleted_files


def main() -> int:
    now = datetime.now()
    default_year = now.year
    default_month = now.month
    run_date_str = now.strftime("%d%m%Y")

    parser = argparse.ArgumentParser(description="Tool crawl_gl_detail: Trích xuất Chi tiết Tất cả Chỉ tiêu Thành phần các Đơn vị Gia Lai")
    parser.add_argument("--time-type", choices=["month", "quarter", "year"], default="year", help="Loại thời gian (mặc định: year)")
    parser.add_argument("--year", type=int, default=default_year, help=f"Năm thống kê (mặc định: {default_year})")
    parser.add_argument("--period", type=int, default=default_month, help="Tháng (1-12) hoặc Quý (1-4)")
    parser.add_argument("--clean-days", type=int, default=3, help="Tự động xóa dữ liệu cũ quá N ngày (mặc định: 3)")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR, help="Thư mục lưu file JSON điểm số & Index")
    parser.add_argument("--raw-dir", type=Path, default=DEFAULT_RAW_DIR, help="Thư mục lưu file RAW JSON")
    parser.add_argument("--concurrency", type=int, default=5, help="Số lượng worker luồng chạy song song (mặc định: 5)")
    parser.add_argument("--delay-min", type=float, default=1.0, help="Thời gian nghỉ tối thiểu tính theo giây (mặc định: 1.0s)")
    parser.add_argument("--delay-max", type=float, default=2.5, help="Thời gian nghỉ tối đa tính theo giây (mặc định: 2.5s)")
    parser.add_argument("--timeout", type=int, default=45, help="Thời gian chờ socket timeout (mặc định: 45s)")
    parser.add_argument("--max-retries", type=int, default=6, help="Số lần thử lại tối đa (mặc định: 6)")

    args = parser.parse_args()

    if args.concurrency < 1:
        args.concurrency = 1

    period = args.period if args.time_type in ("month", "quarter") else None

    # Auto clean
    deleted = clean_old_snapshots(args.output_dir, args.raw_dir, retention_days=args.clean_days)
    if deleted > 0:
        print(f"🧹 Đã auto-clean {deleted} file snapshot chi tiết cũ quá {args.clean_days} ngày.")

    chk_dir = args.raw_dir / str(args.year) / "gia_lai" / "checkpoints"
    chk_dir.mkdir(parents=True, exist_ok=True)
    chk_detail_file = chk_dir / f"checkpoint_detail_{run_date_str}.json"

    print(f"🔄 Đang khởi tạo trích xuất DỮ LIỆU CHI TIẾT TỪNG CHỈ TIÊU Gia Lai (Mốc ngày: {run_date_str})...")
    print(f"⚡ Chế độ thực thi: Multi-Threaded Partition ({args.concurrency} workers) | Jitter: {args.delay_min}s-{args.delay_max}s | Retries: {args.max_retries}")
    print(f"💾 Checkpoint File: {chk_detail_file}")

    pbar = CrawlerProgressBar(total=7, desc="Trích xuất 7 API Chi tiết Gia Lai", unit="endpoint")

    try:
        raw_endpoints = fetch_all_detailed_endpoints(
            args.time_type,
            args.year,
            period,
            checkpoint_file=chk_detail_file,
            timeout=args.timeout,
            max_retries=args.max_retries,
            delay_min=args.delay_min,
            delay_max=args.delay_max,
            concurrency=args.concurrency,
            pbar=pbar,
        )
        pbar.close()

        raw_detail_file = args.raw_dir / str(args.year) / "gia_lai" / f"raw_GiaLai_detail_{run_date_str}.json"
        write_json(raw_detail_file, raw_endpoints)

        processed = process_detailed_units(raw_endpoints)

        period_str = f"{args.time_type.capitalize()} {period}/{args.year}" if period else f"Năm {args.year}"
        metadata = {
            "province": GIA_LAI_NAME,
            "provinceCode": GIA_LAI_CODE,
            "rootDepartmentId": GIA_LAI_ROOT_ID,
            "runDateStr": run_date_str,
            "timeType": args.time_type,
            "year": args.year,
            "period": period,
            "periodLabel": period_str,
            "extractedAt": utc_now(),
        }

        details_file = args.output_dir / f"details_GiaLai_{run_date_str}.json"
        agencies_detail_file = args.output_dir / f"agencies_detail_GiaLai_{run_date_str}.json"
        communes_detail_file = args.output_dir / f"communes_detail_GiaLai_{run_date_str}.json"

        write_json(details_file, {
            "metadata": metadata,
            "overview": processed["overview"],
            "totalUnitsCount": len(processed["units"]),
            "agencyCount": len(processed["agencies"]),
            "communeCount": len(processed["communes"]),
            "unitsDetail": processed["units"],
        })

        write_json(agencies_detail_file, {
            "metadata": metadata,
            "count": len(processed["agencies"]),
            "agenciesDetail": processed["agencies"],
        })

        write_json(communes_detail_file, {
            "metadata": metadata,
            "count": len(processed["communes"]),
            "communesDetail": processed["communes"],
        })

        index_file, index_detail_file = update_api_index_detail(args.output_dir, processed, metadata, run_date_str)

        print("\n" + "=" * 115)
        print(f"📊 BÁO CÁO CHI TIẾT TẤT CẢ CHỈ TIÊU THÀNH PHẦN - {GIA_LAI_NAME.upper()} (Mốc ngày: {run_date_str})")
        print("=" * 115)
        print(f"✅ Đã lưu File RAW Chi tiết: {raw_detail_file}")
        print(f"✅ Đã lưu JSON Chi tiết 149 Đơn vị: {details_file}")
        print(f"✅ Đã lưu JSON Chi tiết Khối Sở/Ngành: {agencies_detail_file}")
        print(f"✅ Đã lưu JSON Chi tiết Khối Xã/Phường: {communes_detail_file}")
        print(f"🚀 Đã Cập nhật Master API Index File: {index_file}")
        print(f"🚀 Đã Cập nhật Master Detailed API Index Database: {index_detail_file}")
        print("=" * 115 + "\n")

    except Exception as exc:
        pbar.close()
        print(f"❌ Lỗi khi trích xuất dữ liệu chi tiết Gia Lai: {exc}", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
