#!/usr/bin/env python3
"""
Tool crawl-gl: Trích xuất, Build API Index & So sánh Điểm số DVCQG UBND Tỉnh Gia Lai.

Endpoint API: https://dichvucong.gov.vn/api/v1/reporting/evaluation/service-results
Province: UBND tỉnh Gia Lai (Code: H21, rootDepartmentId: 019d2be3-6a85-74ec-a346-6489e82ae4c7)

Tính năng cao cấp:
1. Cơ chế Đảo User-Agent & Evasion (User-Agent Rotation Pool): Xoay vòng User-Agent hiện đại, headers ngẫu nhiên & jitter delay (0.2s-0.5s) tránh bị WAF/Firewall chặn kết nối.
2. Engine So sánh Điểm Đồng bộ Theo Ngày (Daily Comparison): So sánh điểm số, thứ hạng và 6 nhóm chỉ tiêu giữa ngày hôm nay với mốc ngày chạy trước đó.
3. Trích xuất đầy đủ 6 nhóm chỉ tiêu thành phần cho TẤT CẢ 149 đơn vị con (CKMB, TDGQ, ONLINE, TTTT, MDSH, MDHL).
4. Lưu các file dữ liệu theo mốc ngày DDMMYYYY: scores_GiaLai_DDMMYYYY.json, agencies_..., communes_..., comparison_....
5. Tự động Build & Cập nhật file Index API (data/gia_lai/index.json).
6. Tự động dọn dẹp (auto-clean) dữ liệu date cũ hơn 3 ngày (> 72 giờ).

Sử dụng mẫu:
  python tools/crawl_gl.py --time-type month --year 2026 --period 3
  python tools/crawl_gl.py --compare-date 22092026
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
ENDPOINT_TRANSPARENCY = "https://dichvucong.gov.vn/api/v1/reporting/evaluation/transparency"
ENDPOINT_PROGRESS = "https://dichvucong.gov.vn/api/v1/reporting/evaluation/dvc-progress-tree"
ENDPOINT_ONLINE = "https://dichvucong.gov.vn/api/v1/reporting/evaluation/provide-online-tree"
ENDPOINT_PAYMENT = "https://dichvucong.gov.vn/api/v1/reporting/evaluation/formality-online-payment-tree"
ENDPOINT_DIGITIZED = "https://dichvucong.gov.vn/api/v1/reporting/evaluation/dossier-digitized"

GIA_LAI_ROOT_ID = "019d2be3-6a85-74ec-a346-6489e82ae4c7"
GIA_LAI_CODE = "H21"
GIA_LAI_NAME = "UBND tỉnh Gia Lai"

ROOT_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT_DIR / "data"
DEFAULT_OUTPUT_DIR = DATA_DIR / "gia_lai"
DEFAULT_RAW_DIR = DATA_DIR / "raw"

# User-Agent Pool for Evasion / Anti-Blocking
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4.1 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36 Edg/123.0.0.0",
]

try:
    from tqdm import tqdm
    HAS_TQDM = True
except ImportError:
    HAS_TQDM = False


class CrawlerProgressBar:
    """Flexible thread-safe progress bar supporting tqdm with fallback to clean standard console output."""

    def __init__(self, total: int, desc: str = "Crawling", unit: str = "item"):
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
    except Exception:
        return None


def get_random_headers() -> dict[str, str]:
    """Generate rotated HTTP headers to bypass WAF / IP rate limits."""
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
    max_retries: int = 4,
    delay_min: float = 1.0,
    delay_max: float = 2.5,
) -> dict[str, Any]:
    """Fetch DVCQG API endpoint with header rotation, random delay jitter, longer timeout, and retries."""
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

    # Anti-blocking: Add randomized jitter delay (uniform + gaussian-like jitter)
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
            if exc.code in (429, 403, 502, 503, 504) and attempt < max_retries:
                retry_wait = (2.0 * attempt) + random.uniform(1.0, 3.0)
                time.sleep(retry_wait)
                continue
            raise RuntimeError(f"HTTP Error {exc.code}: {err_body[:500]}") from exc
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            if attempt < max_retries:
                retry_wait = (2.0 * attempt) + random.uniform(1.0, 3.0)
                time.sleep(retry_wait)
                continue
            reason = getattr(exc, "reason", str(exc))
            raise RuntimeError(f"Network error connecting to DVCQG: {reason}") from exc

    raise RuntimeError(f"Failed to fetch DVCQG endpoint after {max_retries} attempts")


def fetch_national_gia_lai_group_scores(
    time_type: str,
    year: int,
    period: int | None,
    timeout: int = 45,
    max_retries: int = 4,
    delay_min: float = 1.0,
    delay_max: float = 2.5,
) -> dict[str, float] | None:
    """Fetch national service-results response to obtain Gia Lai's full groupScores."""
    try:
        raw_national = fetch_dvc_endpoint(
            ENDPOINT_SERVICE_RESULTS,
            time_type,
            year,
            period,
            root_dept_id=None,
            timeout=timeout,
            max_retries=max_retries,
            delay_min=delay_min,
            delay_max=delay_max,
        )
        eval_list = raw_national.get("data", {}).get("evaluation", [])
        if isinstance(eval_list, list):
            for row in eval_list:
                if isinstance(row, dict) and row.get("departmentCode") == GIA_LAI_CODE:
                    return row.get("groupScores")
    except Exception:
        pass
    return None


def _fetch_single_group_map(
    group_code: str,
    url: str,
    data_key: str,
    time_type: str,
    year: int,
    period: int | None,
    timeout: int,
    max_retries: int,
    delay_min: float,
    delay_max: float,
    pbar: CrawlerProgressBar | None = None,
) -> tuple[str, dict[str, float]]:
    """Worker task to fetch a single group score endpoint concurrently."""
    result_map: dict[str, float] = {}
    try:
        if pbar:
            pbar.set_postfix_str(f"Fetching {group_code}...")
        resp = fetch_dvc_endpoint(
            url,
            time_type,
            year,
            period,
            timeout=timeout,
            max_retries=max_retries,
            delay_min=delay_min,
            delay_max=delay_max,
        )
        data = resp.get("data", {}) if isinstance(resp, dict) else {}
        items = data.get(data_key, [])
        if isinstance(items, list):
            for item in items:
                if isinstance(item, dict) and item.get("departmentId"):
                    score_val = item.get("score") if item.get("score") is not None else item.get("totalScore", 0)
                    result_map[item["departmentId"]] = round(float(score_val), 2)
        if pbar:
            pbar.update(1, status=f"{group_code} OK")
    except Exception as exc:
        print(f"\n⚠️  Warning fetching {group_code} group scores: {exc}", file=sys.stderr)
        if pbar:
            pbar.update(1, status=f"{group_code} Warn")

    return group_code, result_map


def fetch_child_units_group_scores_maps(
    time_type: str,
    year: int,
    period: int | None,
    timeout: int = 45,
    max_retries: int = 4,
    delay_min: float = 1.0,
    delay_max: float = 2.5,
    concurrency: int = 5,
    pbar: CrawlerProgressBar | None = None,
) -> dict[str, dict[str, float]]:
    """Fetch all 5 criteria group endpoints with User-Agent rotation and multi-threaded partition assembly."""
    maps: dict[str, dict[str, float]] = {
        "CKMB": {},
        "TDGQ": {},
        "ONLINE": {},
        "TTTT": {},
        "MDSH": {},
    }

    tasks = [
        ("CKMB", ENDPOINT_TRANSPARENCY, "evaluation"),
        ("TDGQ", ENDPOINT_PROGRESS, "children"),
        ("ONLINE", ENDPOINT_ONLINE, "children"),
        ("TTTT", ENDPOINT_PAYMENT, "children"),
        ("MDSH", ENDPOINT_DIGITIZED, "evaluation"),
    ]

    if concurrency <= 1:
        # Sequential processing
        for group_code, url, data_key in tasks:
            g_code, g_map = _fetch_single_group_map(
                group_code, url, data_key, time_type, year, period, timeout, max_retries, delay_min, delay_max, pbar
            )
            maps[g_code] = g_map
    else:
        # Multi-threaded parallel partition execution & assembly (ráp nối)
        max_workers = min(concurrency, len(tasks))
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [
                executor.submit(
                    _fetch_single_group_map,
                    group_code,
                    url,
                    data_key,
                    time_type,
                    year,
                    period,
                    timeout,
                    max_retries,
                    delay_min,
                    delay_max,
                    pbar,
                )
                for group_code, url, data_key in tasks
            ]
            for future in as_completed(futures):
                try:
                    g_code, g_map = future.result()
                    maps[g_code] = g_map
                except Exception as exc:
                    print(f"\n⚠️  Thread partition error: {exc}", file=sys.stderr)

    return maps





def extract_score_data(
    raw_response: dict[str, Any],
    time_type: str,
    year: int,
    period: int | None = None,
    national_group_scores: dict[str, float] | None = None,
    child_group_maps: dict[str, dict[str, float]] | None = None,
    run_date_str: str | None = None,
) -> dict[str, Any]:
    """Extract and normalize evaluation scores & groupScores from raw API response."""
    data = raw_response.get("data")
    if not isinstance(data, dict):
        raise ValueError("Invalid response schema: missing 'data' dictionary")

    overview = data.get("overview") or {}
    evaluation_raw = data.get("evaluation") or []
    if not isinstance(evaluation_raw, list):
        evaluation_raw = []

    radar_list = data.get("radar") or []
    radar_scores: dict[str, float] = {}
    if isinstance(radar_list, list):
        for r in radar_list:
            if isinstance(r, dict) and r.get("code"):
                radar_scores[r["code"]] = round(float(r.get("score", 0.0)), 2)

    provincial_group_scores = national_group_scores or {
        "CKMB": radar_scores.get("CKMB", 0.0),
        "TDGQ": radar_scores.get("TDGQ", 0.0),
        "CLGQ": radar_scores.get("CLGQ", 0.0),
        "TTTT": radar_scores.get("ONLINE_SERVICE", radar_scores.get("TTTT", 0.0)),
        "MDHL": radar_scores.get("MDHL", 0.0),
        "MDSH": radar_scores.get("MDSH", 0.0),
    }

    units: list[dict[str, Any]] = []
    agencies: list[dict[str, Any]] = []
    communes: list[dict[str, Any]] = []

    cg_maps = child_group_maps or {}

    for item in evaluation_raw:
        if not isinstance(item, dict):
            continue
        did = item.get("departmentId")
        name = item.get("departmentName")
        code = item.get("departmentCode")
        child_group = item.get("childGroup")
        total_score = item.get("totalScore")
        ratio = item.get("ratio")
        score_delta = item.get("scoreDelta")

        tot_val = round(float(total_score), 2) if total_score is not None else 0.0

        ckmb_val = cg_maps.get("CKMB", {}).get(did, 0.0)
        tdgq_val = cg_maps.get("TDGQ", {}).get(did, 0.0)
        online_val = cg_maps.get("ONLINE", {}).get(did, 0.0)
        tttt_val = cg_maps.get("TTTT", {}).get(did, 0.0)
        mdsh_val = cg_maps.get("MDSH", {}).get(did, 0.0)

        known_sum = round(ckmb_val + tdgq_val + online_val + tttt_val + mdsh_val, 2)
        mdhl_val = max(0.0, round(tot_val - known_sum, 2)) if tot_val > 0 else 0.0

        item_group_scores = item.get("groupScores") or {
            "CKMB": ckmb_val,
            "TDGQ": tdgq_val,
            "ONLINE": online_val,
            "TTTT": tttt_val,
            "MDSH": mdsh_val,
            "MDHL": mdhl_val,
        }

        unit_info = {
            "departmentId": did,
            "departmentName": name,
            "departmentCode": code,
            "childGroup": child_group,
            "totalScore": round(float(total_score), 2) if total_score is not None else None,
            "ratio": round(float(ratio), 2) if ratio is not None else None,
            "scoreDelta": round(float(score_delta), 2) if score_delta is not None else None,
            "groupScores": item_group_scores,
        }

        units.append(unit_info)
        if child_group == "AGENCY":
            agencies.append(unit_info)
        elif child_group == "COMMUNE":
            communes.append(unit_info)

    units.sort(key=lambda u: (u["totalScore"] is not None, u["totalScore"] or 0), reverse=True)
    agencies.sort(key=lambda u: (u["totalScore"] is not None, u["totalScore"] or 0), reverse=True)
    communes.sort(key=lambda u: (u["totalScore"] is not None, u["totalScore"] or 0), reverse=True)

    for idx, unit in enumerate(units, 1):
        unit["rank"] = idx

    for idx, unit in enumerate(agencies, 1):
        unit["groupRank"] = idx

    for idx, unit in enumerate(communes, 1):
        unit["groupRank"] = idx

    date_str = run_date_str or datetime.now().strftime("%d%m%Y")
    period_str = f"{time_type.capitalize()} {period}/{year}" if period else f"Năm {year}"

    total_score_val = overview.get("totalScore")
    ratio_val = overview.get("ratio")
    prov_delta = overview.get("scoreDelta")

    return {
        "metadata": {
            "province": GIA_LAI_NAME,
            "provinceCode": GIA_LAI_CODE,
            "rootDepartmentId": GIA_LAI_ROOT_ID,
            "runDateStr": date_str,
            "timeType": time_type,
            "year": year,
            "period": period,
            "periodLabel": period_str,
            "extractedAt": utc_now(),
            "sourceUrl": ENDPOINT_SERVICE_RESULTS,
        },
        "overview": {
            "departmentName": overview.get("departmentName", GIA_LAI_NAME),
            "departmentCode": overview.get("departmentCode", GIA_LAI_CODE),
            "totalScore": round(float(total_score_val), 2) if total_score_val is not None else None,
            "totalMaxScore": overview.get("totalMaxScore", 100),
            "ratio": round(float(ratio_val), 2) if ratio_val is not None else None,
            "scoreDelta": round(float(prov_delta), 2) if prov_delta is not None else None,
            "groupScores": provincial_group_scores,
            "growthPercent": data.get("growthPercent"),
            "totalUnitsCount": len(units),
            "agencyCount": len(agencies),
            "communeCount": len(communes),
        },
        "radar": radar_list,
        "units": units,
        "agencies": agencies,
        "communes": communes,
    }


def find_previous_daily_date(output_dir: Path, current_date_str: str, explicit_compare_date: str | None = None) -> str | None:
    """Find the previous daily date string (DDMMYYYY) for daily comparison."""
    if explicit_compare_date:
        return explicit_compare_date

    # Try reading availableDates from index.json
    index_path = output_dir / "index.json"
    index_data = load_json(index_path)
    if index_data and isinstance(index_data, dict):
        avail = index_data.get("availableDates", [])
        if isinstance(avail, list) and current_date_str in avail:
            idx = avail.index(current_date_str)
            if idx > 0:
                return avail[idx - 1]
        elif isinstance(avail, list) and len(avail) > 0:
            past_dates = [d for d in avail if d < current_date_str]
            if past_dates:
                return past_dates[-1]

    # Fallback to scanning existing score files in output_dir
    score_files = sorted(output_dir.glob("scores_GiaLai_*.json"))
    found_dates: list[str] = []
    for f in score_files:
        m = re.search(r"_(\d{8})\.json$", f.name)
        if m and m.group(1) != current_date_str:
            found_dates.append(m.group(1))

    return found_dates[-1] if found_dates else None


def compare_scores(
    curr_score_data: dict[str, Any],
    prev_score_data: dict[str, Any],
) -> dict[str, Any]:
    """Compare evaluation scores, daily deltas, rank changes & groupScores between current and previous snapshot dates."""
    curr_meta = curr_score_data["metadata"]
    prev_meta = prev_score_data["metadata"]

    curr_overview = curr_score_data["overview"]
    prev_overview = prev_score_data["overview"]

    curr_units = {u["departmentId"]: u for u in curr_score_data["units"] if u.get("departmentId")}
    prev_units = {u["departmentId"]: u for u in prev_score_data["units"] if u.get("departmentId")}

    curr_score_val = curr_overview.get("totalScore")
    prev_score_val = prev_overview.get("totalScore")
    provincial_delta = (
        round(float(curr_score_val) - float(prev_score_val), 2)
        if (curr_score_val is not None and prev_score_val is not None)
        else None
    )

    comparison_units: list[dict[str, Any]] = []
    comp_agencies: list[dict[str, Any]] = []
    comp_communes: list[dict[str, Any]] = []

    increased_count = 0
    decreased_count = 0
    unchanged_count = 0
    new_units_count = 0

    all_dept_ids = list(curr_units.keys())
    all_dept_ids.sort(key=lambda did: curr_units[did].get("rank", 9999))

    for did in all_dept_ids:
        c_unit = curr_units[did]
        p_unit = prev_units.get(did)

        curr_s = c_unit.get("totalScore")
        prev_s = p_unit.get("totalScore") if p_unit else None

        curr_r = c_unit.get("rank")
        prev_r = p_unit.get("rank") if p_unit else None

        c_gs = c_unit.get("groupScores") or {}
        p_gs = p_unit.get("groupScores") or {} if p_unit else {}

        # Calculate GroupScores Deltas
        gs_deltas: dict[str, float] = {}
        for g_code in ["CKMB", "TDGQ", "ONLINE", "TTTT", "MDSH", "MDHL"]:
            if g_code in c_gs and g_code in p_gs:
                gs_deltas[g_code] = round(c_gs[g_code] - p_gs[g_code], 2)

        if prev_s is None:
            delta = None
            trend = "NEW"
            trend_label = "MỚI 🆕"
            rank_change = None
            new_units_count += 1
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
            "departmentId": did,
            "departmentName": c_unit["departmentName"],
            "departmentCode": c_unit["departmentCode"],
            "childGroup": c_unit["childGroup"],
            "currentScore": curr_s,
            "previousScoreDaily": prev_s,
            "scoreDeltaDaily": delta,
            "groupScores": c_gs,
            "groupScoresDeltaDaily": gs_deltas,
            "trend": trend,
            "trendLabel": trend_label,
            "currentRank": curr_r,
            "previousRankDaily": prev_r,
            "groupRank": c_unit.get("groupRank"),
            "rankChangeDaily": rank_change,
        }

        comparison_units.append(comp_record)
        if c_unit["childGroup"] == "AGENCY":
            comp_agencies.append(comp_record)
        elif c_unit["childGroup"] == "COMMUNE":
            comp_communes.append(comp_record)

    valid_deltas = [u for u in comparison_units if u["scoreDeltaDaily"] is not None]
    top_improvers = sorted(valid_deltas, key=lambda u: u["scoreDeltaDaily"], reverse=True)[:5]
    top_drops = sorted(valid_deltas, key=lambda u: u["scoreDeltaDaily"])[:5]

    return {
        "metadata": {
            "province": GIA_LAI_NAME,
            "provinceCode": GIA_LAI_CODE,
            "currentRunDate": curr_meta.get("runDateStr"),
            "previousRunDate": prev_meta.get("runDateStr", "N/A"),
            "currentPeriod": curr_meta["periodLabel"],
            "previousPeriod": prev_meta.get("periodLabel", "N/A"),
            "comparedAt": utc_now(),
        },
        "overview": {
            "currentScore": curr_score_val,
            "previousScoreDaily": prev_score_val,
            "scoreDeltaDaily": provincial_delta,
            "groupScores": curr_overview.get("groupScores", {}),
            "totalUnits": len(comparison_units),
            "agencyCount": len(comp_agencies),
            "communeCount": len(comp_communes),
            "increasedCount": increased_count,
            "decreasedCount": decreased_count,
            "unchangedCount": unchanged_count,
            "newUnitsCount": new_units_count,
        },
        "topImproversDaily": [
            {"name": u["departmentName"], "current": u["currentScore"], "prevDaily": u["previousScoreDaily"], "deltaDaily": u["scoreDeltaDaily"]}
            for u in top_improvers if u["scoreDeltaDaily"] > 0
        ],
        "topDropsDaily": [
            {"name": u["departmentName"], "current": u["currentScore"], "prevDaily": u["previousScoreDaily"], "deltaDaily": u["scoreDeltaDaily"]}
            for u in top_drops if u["scoreDeltaDaily"] < 0
        ],
        "units": comparison_units,
        "agencies": comp_agencies,
        "communes": comp_communes,
    }


def update_api_index(output_dir: Path, score_data: dict[str, Any], date_str: str) -> Path:
    """Build and update the API lookup index file (data/gia_lai/index.json)."""
    index_path = output_dir / "index.json"
    index_data = load_json(index_path) or {}

    if not isinstance(index_data, dict):
        index_data = {}

    index_data["schemaVersion"] = 1
    index_data["updatedAt"] = utc_now()
    index_data["latestDate"] = date_str

    province_info = index_data.setdefault("province", {})
    province_info["name"] = GIA_LAI_NAME
    province_info["code"] = GIA_LAI_CODE
    province_info["id"] = GIA_LAI_ROOT_ID

    avail_dates = set(index_data.get("availableDates") or [])
    avail_dates.add(date_str)
    index_data["availableDates"] = sorted(list(avail_dates))

    ov_hist = index_data.setdefault("overviewHistory", {})
    ov_hist[date_str] = {
        "date": date_str,
        "periodLabel": score_data["metadata"]["periodLabel"],
        "totalScore": score_data["overview"].get("totalScore"),
        "ratio": score_data["overview"].get("ratio"),
        "scoreDelta": score_data["overview"].get("scoreDelta"),
        "groupScores": score_data["overview"].get("groupScores"),
        "totalUnitsCount": score_data["overview"].get("totalUnitsCount"),
        "agencyCount": score_data["overview"].get("agencyCount"),
        "communeCount": score_data["overview"].get("communeCount"),
    }

    index_data["agenciesList"] = [
        {"code": a["departmentCode"], "id": a["departmentId"], "name": a["departmentName"], "rank": a.get("groupRank"), "score": a.get("totalScore"), "groupScores": a.get("groupScores")}
        for a in score_data.get("agencies", [])
    ]
    index_data["communesList"] = [
        {"code": c["departmentCode"], "id": c["departmentId"], "name": c["departmentName"], "rank": c.get("groupRank"), "score": c.get("totalScore"), "groupScores": c.get("groupScores")}
        for c in score_data.get("communes", [])
    ]

    dept_index = index_data.setdefault("departmentsIndex", {})
    for unit in score_data.get("units", []):
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
            "history": {},
        })

        unit_record = {
            "date": date_str,
            "periodLabel": score_data["metadata"]["periodLabel"],
            "totalScore": unit.get("totalScore"),
            "ratio": unit.get("ratio"),
            "scoreDelta": unit.get("scoreDelta"),
            "rank": unit.get("rank"),
            "groupRank": unit.get("groupRank"),
            "groupScores": unit.get("groupScores"),
        }

        d_entry["latest"] = unit_record
        d_entry.setdefault("history", {})[date_str] = unit_record

    write_json(index_path, index_data)
    return index_path


def parse_date_str(date_str: str) -> datetime | None:
    """Parse DDMMYYYY string to datetime object."""
    if not isinstance(date_str, str) or len(date_str) != 8:
        return None
    try:
        return datetime.strptime(date_str, "%d%m%Y")
    except ValueError:
        return None


def clean_old_snapshots(output_dir: Path, raw_dir: Path, retention_days: int = 3) -> dict[str, int]:
    """Automatically remove snapshot files and index entries older than retention_days (default: 3 days)."""
    cutoff_date = datetime.now() - timedelta(days=retention_days)
    deleted_files = 0
    purged_dates: list[str] = []

    patterns = [
        (output_dir, "scores_GiaLai_*.json"),
        (output_dir, "agencies_GiaLai_*.json"),
        (output_dir, "communes_GiaLai_*.json"),
        (output_dir, "comparison_GiaLai_*.json"),
        (raw_dir / "2026" / "gia_lai", "raw_GiaLai_*.json"),
    ]

    for parent_dir, pattern in patterns:
        if not parent_dir.exists():
            continue
        for fpath in parent_dir.glob(pattern):
            match = re.search(r"_(\d{8})\.json$", fpath.name)
            if not match:
                continue
            d_str = match.group(1)
            file_dt = parse_date_str(d_str)
            if file_dt and file_dt < cutoff_date.replace(hour=0, minute=0, second=0, microsecond=0):
                try:
                    fpath.unlink()
                    deleted_files += 1
                    if d_str not in purged_dates:
                        purged_dates.append(d_str)
                except Exception as exc:
                    print(f"⚠️  Could not delete old file {fpath}: {exc}", file=sys.stderr)

    index_path = output_dir / "index.json"
    index_data = load_json(index_path)
    if index_data and isinstance(index_data, dict):
        avail_dates = index_data.get("availableDates", [])
        valid_dates: list[str] = []
        purged_set: set[str] = set()

        for d_str in avail_dates:
            dt = parse_date_str(d_str)
            if dt and dt < cutoff_date.replace(hour=0, minute=0, second=0, microsecond=0):
                purged_set.add(d_str)
            else:
                valid_dates.append(d_str)

        if purged_set:
            index_data["availableDates"] = valid_dates
            ov_hist = index_data.get("overviewHistory", {})
            for d in list(ov_hist.keys()):
                if d in purged_set:
                    ov_hist.pop(d, None)

            dept_idx = index_data.get("departmentsIndex", {})
            for dept_info in dept_idx.values():
                hist = dept_info.get("history", {})
                for d in list(hist.keys()):
                    if d in purged_set:
                        hist.pop(d, None)

            write_json(index_path, index_data)

    return {"deletedFiles": deleted_files, "purgedDates": len(purged_dates)}


def render_console_table(comp_data: dict[str, Any], group_filter: str = "ALL", limit: int | None = None) -> None:
    """Print formatted comparative score table and overview to terminal."""
    meta = comp_data["metadata"]
    ov = comp_data["overview"]
    units = comp_data["units"]

    if group_filter == "AGENCY":
        units = comp_data.get("agencies", [u for u in units if u["childGroup"] == "AGENCY"])
        group_title = "KHỐI SỞ BAN NGÀNH (AGENCY)"
    elif group_filter == "COMMUNE":
        units = comp_data.get("communes", [u for u in units if u["childGroup"] == "COMMUNE"])
        group_title = "KHỐI UBND XÃ / PHƯỜNG / THỊ TRẤN (COMMUNE)"
    else:
        group_title = "TẤT CẢ CÁC ĐƠN VỊ CON (AGENCY & COMMUNE)"

    if limit and limit > 0:
        units = units[:limit]

    print("\n" + "=" * 115)
    print(f"📊 BÁO CÁO KẾT QUẢ VÀ SO SÁNH ĐIỂM SỐ CHẤT LƯỢNG PHỤC VỤ - {meta['province'].upper()}")
    print(f"📌 Lọc hiển thị: {group_title}  |  Mốc ngày chạy: {meta.get('currentRunDate')} (vs Ngày: {meta.get('previousRunDate')})")
    print("=" * 115)

    prov_prev_str = f"Mốc ngày {meta.get('previousRunDate')}: {ov['previousScoreDaily']:.2f}" if ov.get('previousScoreDaily') is not None else "Khuyết mốc ngày trước"
    prov_delta_str = f"{ov['scoreDeltaDaily']:+g}" if ov.get('scoreDeltaDaily') is not None else "-"

    print(f"🏛️  Điểm UBND Tỉnh Gia Lai: {ov['currentScore']} ({prov_prev_str} | Chênh lệch ngày: {prov_delta_str} điểm)")

    g_scores = ov.get("groupScores") or {}
    if g_scores:
        g_str = ", ".join([f"{k}: {v}" for k, v in g_scores.items()])
        print(f"🧩 Chi tiết 6 nhóm chỉ tiêu Gia Lai: {g_str}")

    print(
        f"📈 Đơn vị Tăng điểm: {ov['increasedCount']}  |  📉 Đơn vị Giảm điểm: {ov['decreasedCount']}  |  "
        f"➡️  Không đổi: {ov['unchangedCount']}  |  🆕 Mới: {ov['newUnitsCount']}"
    )
    print("-" * 115)

    header_fmt = "{:<5} | {:<38} | {:<9} | {:<10} | {:<10} | {:<10} | {:<9}"
    print(header_fmt.format("STT", "Tên đơn vị con", "Nhóm", "Điểm Trước", "Điểm Này", "Chênh Ngày", "Xu hướng"))
    print("-" * 115)

    for idx, u in enumerate(units, 1):
        group_tag = "Sở/Ngành" if u["childGroup"] == "AGENCY" else ("Xã/Phường" if u["childGroup"] == "COMMUNE" else "Khác")
        prev_str = f"{u['previousScoreDaily']:.2f}" if u.get("previousScoreDaily") is not None else "-"
        curr_str = f"{u['currentScore']:.2f}" if u["currentScore"] is not None else "-"
        delta_str = f"{u['scoreDeltaDaily']:+.2f}" if u.get("scoreDeltaDaily") is not None else "NEW"

        name_str = u["departmentName"]
        if len(name_str) > 38:
            name_str = name_str[:35] + "..."

        print(header_fmt.format(idx, name_str, group_tag, prev_str, curr_str, delta_str, u["trendLabel"]))

    print("-" * 115)
    if group_filter == "ALL" and comp_data.get("topImproversDaily"):
        print("\n🏆 TOP ĐƠN VỊ TĂNG ĐIỂM BỨC PHÁ THEO NGÀY:")
        for item in comp_data["topImproversDaily"]:
            print(f"  • {item['name']}: {item['prevDaily']} ➡️ {item['current']} ({item['deltaDaily']:+.2f} điểm)")

    if group_filter == "ALL" and comp_data.get("topDropsDaily"):
        print("\n⚠️ TOP ĐƠN VỊ GIẢM ĐIỂM CẦN LƯU Ý THEO NGÀY:")
        for item in comp_data["topDropsDaily"]:
            print(f"  • {item['name']}: {item['prevDaily']} ➡️ {item['current']} ({item['deltaDaily']:+.2f} điểm)")

    print("=" * 115 + "\n")


def main() -> int:
    now = datetime.now()
    default_year = now.year
    default_month = now.month
    run_date_str = now.strftime("%d%m%Y")

    parser = argparse.ArgumentParser(description="Tool crawl-gl: Trích xuất, Build Index & So sánh Điểm số Gia Lai")
    parser.add_argument("--time-type", choices=["month", "quarter", "year"], default="year", help="Loại thời gian (mặc định: year)")
    parser.add_argument("--year", type=int, default=default_year, help=f"Năm thống kê (mặc định: {default_year})")
    parser.add_argument("--period", type=int, default=default_month, help="Tháng (1-12) hoặc Quý (1-4)")
    parser.add_argument("--compare-date", help="Mốc ngày so sánh tính theo DDMMYYYY (tùy chọn)")
    parser.add_argument("--group", choices=["ALL", "AGENCY", "COMMUNE"], default="ALL", help="Lọc nhóm hiển thị")
    parser.add_argument("--limit", type=int, help="Giới hạn số dòng hiển thị trên bảng console")
    parser.add_argument("--clean-days", type=int, default=3, help="Tự động xóa dữ liệu snapshot cũ quá N ngày (mặc định: 3)")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR, help="Thư mục lưu file JSON điểm số & Index")
    parser.add_argument("--raw-dir", type=Path, default=DEFAULT_RAW_DIR, help="Thư mục lưu file RAW JSON")
    parser.add_argument("--concurrency", type=int, default=5, help="Số lượng worker luồng chạy song song tự phân tách & ráp nối dữ liệu (mặc định: 5)")
    parser.add_argument("--delay-min", type=float, default=1.0, help="Thời gian nghỉ tối thiểu giữa các request tính theo giây (mặc định: 1.0s)")
    parser.add_argument("--delay-max", type=float, default=2.5, help="Thời gian nghỉ tối đa giữa các request tính theo giây (mặc định: 2.5s)")
    parser.add_argument("--timeout", type=int, default=45, help="Thời gian chờ socket timeout cho mỗi HTTP request (mặc định: 45s)")
    parser.add_argument("--max-retries", type=int, default=4, help="Số lần thử lại tối đa khi gặp lỗi mạng/timeout (mặc định: 4)")
    parser.add_argument("--quiet", action="store_true", help="Không in bảng console, chỉ xuất JSON")

    args = parser.parse_args()

    if args.delay_max < args.delay_min:
        raise SystemExit("--delay-max phải lớn hơn hoặc bằng --delay-min")

    if args.concurrency < 1:
        args.concurrency = 1

    period = args.period if args.time_type in ("month", "quarter") else None

    # 1. Execute auto-clean for snapshots older than args.clean_days
    clean_result = clean_old_snapshots(args.output_dir, args.raw_dir, retention_days=args.clean_days)
    if clean_result["deletedFiles"] > 0:
        print(f"🧹 Đã auto-clean {clean_result['deletedFiles']} file snapshot cũ quá {args.clean_days} ngày.")

    raw_curr_file = args.raw_dir / str(args.year) / "gia_lai" / f"raw_GiaLai_{run_date_str}.json"
    scores_curr_file = args.output_dir / f"scores_GiaLai_{run_date_str}.json"
    agencies_curr_file = args.output_dir / f"agencies_GiaLai_{run_date_str}.json"
    communes_curr_file = args.output_dir / f"communes_GiaLai_{run_date_str}.json"
    comparison_file = args.output_dir / f"comparison_GiaLai_{run_date_str}.json"

    print(f"🔄 Đang khởi tạo trích xuất điểm số Gia Lai (Mốc ngày: {run_date_str})...")
    if args.concurrency > 1:
        print(f"⚡ Chế độ thực thi: Multi-Threaded Partition & Assembly ({args.concurrency} workers | Tự phân tách & ráp nối dữ liệu)")
    else:
        print(f"🔒 Chế độ thực thi: Single-Threaded Sequential (1 worker)")
    print(f"⏱️  Phân phối ngẫu nhiên thời gian nghỉ (Jitter Sleep): {args.delay_min}s ➡️ {args.delay_max}s | Timeout: {args.timeout}s | Retries: {args.max_retries}")

    # Initialize Crawler Progress Bar (Total 7 endpoints: 1 national + 5 criteria groups + 1 main service results)
    pbar = CrawlerProgressBar(total=7, desc="Trích xuất API Endpoints Gia Lai", unit="endpoint")

    try:
        # Fetch National GroupScores for Gia Lai Overview
        pbar.set_postfix_str("Fetching National Gia Lai Overview...")
        national_group_scores = fetch_national_gia_lai_group_scores(
            args.time_type,
            args.year,
            period,
            timeout=args.timeout,
            max_retries=args.max_retries,
            delay_min=args.delay_min,
            delay_max=args.delay_max,
        )
        pbar.update(1, status="National Scores OK")

        # Fetch Child Units Group Scores Maps across all 5 criteria endpoints (Parallel partition & assembly)
        child_group_maps = fetch_child_units_group_scores_maps(
            args.time_type,
            args.year,
            period,
            timeout=args.timeout,
            max_retries=args.max_retries,
            delay_min=args.delay_min,
            delay_max=args.delay_max,
            concurrency=args.concurrency,
            pbar=pbar,
        )


        # Fetch Current Period Main Data
        pbar.set_postfix_str("Fetching Gia Lai Main Service Results...")
        raw_curr_data = fetch_dvc_endpoint(
            ENDPOINT_SERVICE_RESULTS,
            args.time_type,
            args.year,
            period,
            timeout=args.timeout,
            max_retries=args.max_retries,
            delay_min=args.delay_min,
            delay_max=args.delay_max,
        )

        write_json(raw_curr_file, raw_curr_data)
        pbar.update(1, status="Main Service Results OK")
        pbar.close()

        curr_score_data = extract_score_data(
            raw_curr_data,
            args.time_type,
            args.year,
            period,
            national_group_scores=national_group_scores,
            child_group_maps=child_group_maps,
            run_date_str=run_date_str,
        )
        write_json(scores_curr_file, curr_score_data)

        write_json(agencies_curr_file, {
            "metadata": curr_score_data["metadata"],
            "overview": curr_score_data["overview"],
            "count": len(curr_score_data["agencies"]),
            "agencies": curr_score_data["agencies"],
        })
        write_json(communes_curr_file, {
            "metadata": curr_score_data["metadata"],
            "overview": curr_score_data["overview"],
            "count": len(curr_score_data["communes"]),
            "communes": curr_score_data["communes"],
        })

        print(f"✅ Đã lưu Score JSON tổng hợp: {scores_curr_file}")
        print(f"✅ Đã lưu Score JSON Khối Sở/Ngành: {agencies_curr_file}")
        print(f"✅ Đã lưu Score JSON Khối UBND Xã/Phường: {communes_curr_file}")

        index_file = update_api_index(args.output_dir, curr_score_data, run_date_str)
        print(f"🚀 Đã Cập nhật API Index Database: {index_file}")

    except Exception as exc:
        pbar.close()
        print(f"❌ Lỗi khi tải dữ liệu mốc hiện tại: {exc}", file=sys.stderr)
        return 1


    # Daily Comparison: Find previous stored daily date
    prev_date_str = find_previous_daily_date(args.output_dir, run_date_str, args.compare_date)

    if prev_date_str:
        print(f"🔄 Đang so sánh dữ liệu mốc ngày {run_date_str} với mốc ngày trước đó ({prev_date_str})...")
        prev_file = args.output_dir / f"scores_GiaLai_{prev_date_str}.json"
        prev_score_data = load_json(prev_file)

        if prev_score_data is None:
            prev_score_data = {
                "metadata": {"runDateStr": prev_date_str, "periodLabel": "N/A"},
                "overview": {},
                "units": [],
                "agencies": [],
                "communes": [],
            }
    else:
        print("ℹ️  Chưa tìm thấy mốc ngày chạy trước đó. Khởi tạo dữ liệu so sánh kỳ đầu tiên.")
        prev_score_data = {
            "metadata": {"runDateStr": "N/A", "periodLabel": "N/A"},
            "overview": {},
            "units": [],
            "agencies": [],
            "communes": [],
        }

    comp_data = compare_scores(curr_score_data, prev_score_data)
    write_json(comparison_file, comp_data)
    print(f"✅ Đã tạo & lưu file So sánh Điểm số theo Ngày: {comparison_file}")

    if not args.quiet:
        render_console_table(comp_data, group_filter=args.group, limit=args.limit)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
