#!/usr/bin/env python3
"""Reliable DVCQG province crawler.

Design goals
------------
* One fresh Chromium browser context per province to reduce session/WAF carry-over.
* Prefer the live DVCQG UI and capture the matching service-results response.
* Fall back to a browser-context POST when the UI cannot be driven.
* Never accept the national "Cả nước" response as a province response.
* Save each successful province response verbatim as RAW JSON immediately.
* Persist checkpoint state after every province so Ctrl+C/resume is safe.
* A failed province never terminates the whole 34-province run.
* Rebuild catalog JSON from all successful RAW files, so resume does not lose old data.

Examples
--------
python tools/crawl_catalog.py --year 2026 --province-code H20 --headed
python tools/crawl_catalog.py --year 2026 --all
python tools/crawl_catalog.py --year 2026 --all --resume
python tools/crawl_catalog.py --year 2026 --all --force
python tools/crawl_catalog.py --year 2026 --all --retry-failed
"""

from __future__ import annotations

import argparse
import hashlib
import json
import random
import re
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import Response, sync_playwright

ROOT = Path(__file__).resolve().parents[1]
SOURCE_URL = "https://dichvucong.gov.vn/danh-gia-chat-luong-phuc-vu"
ENDPOINT = "https://dichvucong.gov.vn/api/v1/reporting/evaluation/service-results"
PROVINCE_DISCOVERY = ROOT / "data" / "config" / "departments.json"


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def normal_name(value: Any) -> str:
    return " ".join(str(value or "").split()).strip()


def province_short_name(name: str) -> str:
    value = normal_name(name)
    value = re.sub(r"^UBND\s+", "", value, flags=re.I)
    value = re.sub(r"^tỉnh\s+", "", value, flags=re.I)
    value = re.sub(r"^thành phố\s+", "", value, flags=re.I)
    return value.strip()


def safe_preview(text: str, limit: int = 300) -> str:
    return text[:limit].replace("\r", " ").replace("\n", " ")


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def load_provinces() -> list[dict[str, Any]]:
    if not PROVINCE_DISCOVERY.exists():
        raise FileNotFoundError(f"Missing {PROVINCE_DISCOVERY}. Run discover_dvc_structure.py first.")

    payload = load_json(PROVINCE_DISCOVERY, {})
    rows = payload.get("unclassified", [])
    provinces: list[dict[str, Any]] = []

    for row in rows:
        code = normal_name(row.get("departmentCode"))
        name = normal_name(row.get("departmentName"))
        department_id = normal_name(row.get("departmentId"))
        if not code or not name or not department_id:
            continue
        if row.get("childGroup") is not None:
            continue
        if not code.startswith("H"):
            continue
        provinces.append({
            "departmentId": department_id,
            "departmentName": name,
            "departmentCode": code,
            "provinceShortName": province_short_name(name),
        })

    provinces.sort(key=lambda x: x["departmentCode"])
    dedup: dict[str, dict[str, Any]] = {}
    for row in provinces:
        dedup.setdefault(row["departmentCode"], row)
    return list(dedup.values())


def parse_response(text: str) -> dict[str, Any]:
    if not text or not text.strip():
        raise ValueError("API response body is empty")
    payload = json.loads(text)
    if not isinstance(payload, dict):
        raise ValueError("API response is not a JSON object")
    if not isinstance(payload.get("data"), dict):
        raise ValueError("API response missing object field data")
    return payload


def response_matches_province(text: str, province: dict[str, Any]) -> bool:
    """Return True only if the JSON response can be tied to the requested province."""
    try:
        payload = parse_response(text)
    except Exception:
        return False

    data = payload.get("data", {})
    overview = data.get("overview") or {}
    expected_code = province["departmentCode"]
    expected_name = province["departmentName"]

    overview_code = normal_name(overview.get("departmentCode"))
    overview_name = normal_name(overview.get("departmentName"))
    if overview_code and overview_code == expected_code:
        return True
    if overview_name and overview_name == expected_name:
        return True

    evaluation = data.get("evaluation") or []
    if isinstance(evaluation, list):
        for item in evaluation:
            if not isinstance(item, dict):
                continue
            if normal_name(item.get("departmentCode")) == expected_code:
                return True
            if normal_name(item.get("departmentName")) == expected_name:
                return True
    return False


def verify_and_extract(province: dict[str, Any], response_payload: dict[str, Any]) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]], list[str]]:
    data = response_payload["data"]
    overview = data.get("overview") or {}
    evaluation = data.get("evaluation") or []
    if not isinstance(evaluation, list):
        raise ValueError("data.evaluation is not an array")

    expected_code = province["departmentCode"]
    expected_name = province["departmentName"]
    overview_code = normal_name(overview.get("departmentCode"))
    overview_name = normal_name(overview.get("departmentName"))

    if overview_code and overview_code != expected_code:
        raise ValueError(f"Province verification failed: requested {expected_code} but overview is {overview_code}")
    if overview_name and overview_name != expected_name:
        raise ValueError(f"Province verification failed: requested '{expected_name}' but overview is '{overview_name}'")

    province_records: list[dict[str, Any]] = []
    agencies: list[dict[str, Any]] = []
    communes: list[dict[str, Any]] = []
    warnings: list[str] = []

    for item in evaluation:
        if not isinstance(item, dict):
            continue
        record = {
            **item,
            "provinceCode": expected_code,
            "provinceName": expected_name,
            "rootDepartmentId": province["departmentId"],
        }
        child_group = item.get("childGroup")
        department_code = normal_name(item.get("departmentCode"))

        if child_group == "AGENCY":
            agencies.append(record)
        elif child_group == "COMMUNE":
            communes.append(record)
        elif department_code == expected_code:
            province_records.append(record)
        elif child_group is None and not department_code:
            warnings.append(f"Unclassified record without code: {item.get('departmentName')}")
        else:
            warnings.append(
                f"Unclassified evaluation record: {item.get('departmentCode')} / {item.get('departmentName')} / childGroup={child_group}"
            )

    if not province_records:
        warnings.append("No province-level record was found inside data.evaluation.")

    return (
        {
            "departmentId": province["departmentId"],
            "departmentName": expected_name,
            "departmentCode": expected_code,
            "rootDepartmentId": province["departmentId"],
            "verified": True,
            "overview": overview,
            "source": "service-results",
        },
        agencies,
        communes,
        warnings,
    )


def load_checkpoint(path: Path) -> dict[str, Any]:
    payload = load_json(path, {})
    if not isinstance(payload, dict):
        payload = {}
    payload.setdefault("schemaVersion", 1)
    payload.setdefault("updatedAt", None)
    payload.setdefault("items", {})
    return payload


def save_checkpoint(path: Path, checkpoint: dict[str, Any]) -> None:
    checkpoint["updatedAt"] = utc_now()
    write_json(path, checkpoint)


def classify_status_from_checkpoint(item: dict[str, Any] | None) -> str | None:
    if not isinstance(item, dict):
        return None
    status = item.get("status")
    return status if isinstance(status, str) else None


def rebuild_catalog(year: int, provinces: list[dict[str, Any]], raw_dir: Path, catalog_dir: Path, checkpoint: dict[str, Any]) -> dict[str, int]:
    verified_provinces: list[dict[str, Any]] = []
    agencies: list[dict[str, Any]] = []
    communes: list[dict[str, Any]] = []

    for province in provinces:
        code = province["departmentCode"]
        raw_path = raw_dir / f"{code}.json"
        if not raw_path.exists():
            continue
        text = raw_path.read_text(encoding="utf-8")
        try:
            payload = parse_response(text)
            province_record, province_agencies, province_communes, warnings = verify_and_extract(province, payload)
        except Exception:
            continue
        verified_provinces.append(province_record)
        agencies.extend(province_agencies)
        communes.extend(province_communes)
        if code in checkpoint["items"]:
            checkpoint["items"][code]["catalogWarnings"] = warnings

    generated_at = utc_now()
    write_json(catalog_dir / "provinces.json", {
        "schemaVersion": 3,
        "generatedAt": generated_at,
        "year": year,
        "source": SOURCE_URL,
        "count": len(verified_provinces),
        "provinces": verified_provinces,
    })
    write_json(catalog_dir / "agencies.json", {
        "schemaVersion": 3,
        "generatedAt": generated_at,
        "year": year,
        "count": len(agencies),
        "agencies": agencies,
    })
    write_json(catalog_dir / "communes.json", {
        "schemaVersion": 3,
        "generatedAt": generated_at,
        "year": year,
        "count": len(communes),
        "communes": communes,
    })

    checkpoint["catalogSummary"] = {
        "verifiedProvinces": len(verified_provinces),
        "agencies": len(agencies),
        "communes": len(communes),
    }
    return checkpoint["catalogSummary"]


def write_manifest(year: int, endpoint: str, checkpoint: dict[str, Any], catalog_summary: dict[str, int], catalog_dir: Path) -> None:
    items = checkpoint.get("items", {})
    verified = sum(1 for item in items.values() if item.get("status") == "verified")
    failed = sum(1 for item in items.values() if item.get("status") not in ("verified", None))
    write_json(catalog_dir / "crawl-manifest.json", {
        "schemaVersion": 2,
        "generatedAt": utc_now(),
        "year": year,
        "endpoint": endpoint,
        "requested": len(items),
        "verified": verified,
        "failed": failed,
        "items": items,
        "catalogSummary": catalog_summary,
    })


def click_province_in_ui(page, province: dict[str, Any]) -> bool:
    short_name = province["provinceShortName"]
    full_name = province["departmentName"]

    selectors = [
        page.get_by_role("button", name=re.compile(r"Tỉnh,? Thành phố", re.I)).first,
        page.get_by_text(re.compile(r"^Tỉnh,? Thành phố$", re.I)).first,
    ]
    opened = False
    for locator in selectors:
        try:
            if locator.count() and locator.is_visible():
                locator.click(timeout=2500)
                page.wait_for_timeout(700)
                opened = True
                break
        except Exception:
            pass

    if not opened:
        try:
            locator = page.locator('button,[role="button"]').filter(has_text=re.compile(r"Tỉnh|Thành phố|Địa phương", re.I)).first
            if locator.count() and locator.is_visible():
                locator.click(timeout=2500)
                page.wait_for_timeout(700)
                opened = True
        except Exception:
            pass

    if not opened:
        return False

    candidates = [
        full_name,
        short_name,
        re.sub(r"^UBND\s+", "", full_name, flags=re.I),
        re.sub(r"^UBND\s+(tỉnh|Thành phố)\s+", "", full_name, flags=re.I),
    ]
    for candidate in candidates:
        candidate = normal_name(candidate)
        if not candidate:
            continue
        for locator in [
            page.get_by_role("option", name=re.compile(f"^{re.escape(candidate)}$", re.I)).first,
            page.get_by_text(re.compile(f"^{re.escape(candidate)}$", re.I)).last,
        ]:
            try:
                if locator.count() and locator.is_visible():
                    locator.click(timeout=2500)
                    page.wait_for_timeout(500)
                    return True
            except Exception:
                pass
    return False


def capture_matching_ui_response(page, province: dict[str, Any], timeout_ms: int) -> tuple[int, str, str] | None:
    captured: dict[str, Any] = {}

    def on_response(response: Response) -> None:
        if ENDPOINT not in response.url:
            return
        try:
            text = response.text()
        except Exception:
            return
        if not text.lstrip().startswith(("{", "[")):
            return
        if not response_matches_province(text, province):
            return
        captured["status"] = response.status
        captured["text"] = text
        captured["content_type"] = response.headers.get("content-type", "")

    page.on("response", on_response)
    try:
        if not click_province_in_ui(page, province):
            return None
        deadline = time.time() + timeout_ms / 1000
        while time.time() < deadline:
            if captured.get("text"):
                return int(captured["status"]), str(captured["text"]), str(captured.get("content_type", ""))
            page.wait_for_timeout(250)
        return None
    finally:
        try:
            page.remove_listener("response", on_response)
        except Exception:
            pass


def browser_fetch(page, province: dict[str, Any], year: int, timeout_ms: int) -> tuple[int, str, str]:
    payload = {"timeType": "year", "year": year, "rootDepartmentId": province["departmentId"]}
    result = page.evaluate(
        """
        async ({url, payload}) => {
          const res = await fetch(url, {
            method: 'POST',
            credentials: 'include',
            headers: {
              'Accept': 'application/json, text/plain, */*',
              'Content-Type': 'application/json'
            },
            body: JSON.stringify(payload)
          });
          const text = await res.text();
          return {status: res.status, contentType: res.headers.get('content-type') || '', text};
        }
        """,
        {"url": ENDPOINT, "payload": payload},
    )
    return int(result["status"]), str(result.get("text", "")), str(result.get("contentType", ""))


def direct_http_fetch(province: dict[str, Any], year: int, timeout: int = 10) -> tuple[int, str, str] | None:
    """Fast-path direct HTTP POST attempt to bypass browser launching overhead/timeouts."""
    payload = {"timeType": "year", "year": year, "rootDepartmentId": province["departmentId"]}
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    ]
    headers = {
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json",
        "User-Agent": random.choice(user_agents),
        "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
        "Referer": SOURCE_URL,
        "Origin": "https://dichvucong.gov.vn",
    }
    request = urllib.request.Request(ENDPOINT, data=body, method="POST", headers=headers)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            status = response.status
            content_type = response.headers.get("Content-Type", "")
            text = response.read().decode("utf-8", errors="replace")
            if status in (200, 201) and text.lstrip().startswith(("{", "[")) and response_matches_province(text, province):
                return status, text, content_type
    except Exception:
        pass
    return None


def crawl_one_province(pw, province: dict[str, Any], year: int, headed: bool, timeout_ms: int, ui_first: bool) -> tuple[bool, dict[str, Any]]:
    # Fast path: Try direct HTTP POST first to avoid Playwright browser startup overhead/hangs
    fast_http = direct_http_fetch(province, year, timeout=min(10, int(timeout_ms / 1000)))
    if fast_http:
        status, text, content_type = fast_http
        payload = parse_response(text)
        province_record, agencies, communes, warnings = verify_and_extract(province, payload)
        return True, {
            "status": "verified",
            "method": "direct-http-post",
            "httpStatus": status,
            "contentType": content_type,
            "responseSha256": sha256_text(text),
            "evaluationCount": len(payload["data"].get("evaluation") or []),
            "agencyCount": len(agencies),
            "communeCount": len(communes),
            "warnings": warnings,
            "provinceRecord": province_record,
            "rawText": text,
        }

    browser = None
    try:
        browser = pw.chromium.launch(headless=not headed)
        context = browser.new_context(locale="vi-VN", viewport={"width": 1440, "height": 1000})
        page = context.new_page()

        try:
            page.goto(SOURCE_URL, wait_until="domcontentloaded", timeout=10_000)
            page.wait_for_timeout(2000)
        except PlaywrightTimeoutError:
            pass

        result: tuple[int, str, str] | None = None
        method = None

        if ui_first:
            try:
                result = capture_matching_ui_response(page, province, timeout_ms)
                if result:
                    method = "ui-response"
            except Exception:
                result = None

        if result is None:
            try:
                status, text, content_type = browser_fetch(page, province, year, timeout_ms)
                if status in (200, 201) and text.lstrip().startswith(("{", "[")) and response_matches_province(text, province):
                    result = (status, text, content_type)
                    method = "browser-fetch"
            except Exception:
                result = None

        if result is None:
            return False, {
                "status": "failed_request",
                "method": method,
                "error": "No matching province response obtained",
            }

        status, text, content_type = result
        if status not in (200, 201):
            return False, {
                "status": "failed_http",
                "httpStatus": status,
                "contentType": content_type,
                "bodyPreview": safe_preview(text),
                "method": method,
            }
        if not response_matches_province(text, province):
            return False, {
                "status": "failed_verification",
                "httpStatus": status,
                "contentType": content_type,
                "bodyPreview": safe_preview(text),
                "method": method,
                "error": "Response did not match requested province",
            }

        payload = parse_response(text)
        province_record, agencies, communes, warnings = verify_and_extract(province, payload)
        return True, {
            "status": "verified",
            "method": method,
            "httpStatus": status,
            "contentType": content_type,
            "responseSha256": sha256_text(text),
            "evaluationCount": len(payload["data"].get("evaluation") or []),
            "agencyCount": len(agencies),
            "communeCount": len(communes),
            "warnings": warnings,
            "provinceRecord": province_record,
            "rawText": text,
        }
    finally:
        if browser is not None:
            try:
                browser.close()
            except Exception:
                pass


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--year", type=int, default=datetime.now().year)
    parser.add_argument("--all", action="store_true", help="Crawl all discovered provinces")
    parser.add_argument("--limit", type=int, default=1, help="Number of provinces when --all is not used")
    parser.add_argument("--province-code", help="Crawl a single province, e.g. H20")
    parser.add_argument("--headed", action="store_true", help="Show Chromium windows")
    parser.add_argument("--ui-first", action="store_true", help="Try the live province selector before browser fetch")
    parser.add_argument("--force", action="store_true", help="Recrawl even if RAW already exists")
    parser.add_argument("--resume", action="store_true", help="Skip provinces already verified in checkpoint")
    parser.add_argument("--retry-failed", action="store_true", help="Crawl only provinces previously marked failed")
    parser.add_argument("--timeout", type=int, default=45_000)
    parser.add_argument("--delay-min", type=float, default=3.0)
    parser.add_argument("--delay-max", type=float, default=6.0)
    parser.add_argument("--retries-per-province", type=int, default=1)
    parser.add_argument("--allow-fail", action="store_true", help="Do not exit with non-zero code on request failure")
    args = parser.parse_args()

    if args.delay_max < args.delay_min:
        raise SystemExit("--delay-max must be >= --delay-min")

    provinces = load_provinces()
    if not provinces:
        raise RuntimeError("No province records found")

    if args.province_code:
        targets = [p for p in provinces if p["departmentCode"] == args.province_code]
        if not targets:
            raise RuntimeError(f"Province code not found: {args.province_code}")
    elif args.all:
        targets = provinces
    else:
        targets = provinces[: max(1, args.limit)]

    raw_dir = ROOT / "data" / "raw" / str(args.year) / "provinces"
    catalog_dir = ROOT / "data" / "catalog" / str(args.year)
    state_dir = ROOT / "data" / "state"
    raw_dir.mkdir(parents=True, exist_ok=True)
    catalog_dir.mkdir(parents=True, exist_ok=True)
    state_dir.mkdir(parents=True, exist_ok=True)

    checkpoint_path = state_dir / f"crawl-{args.year}.json"
    checkpoint = load_checkpoint(checkpoint_path)
    checkpoint["year"] = args.year
    checkpoint["endpoint"] = ENDPOINT

    if args.resume:
        targets = [p for p in targets if classify_status_from_checkpoint(checkpoint["items"].get(p["departmentCode"])) != "verified"]
    elif args.retry_failed:
        targets = [p for p in targets if classify_status_from_checkpoint(checkpoint["items"].get(p["departmentCode"])) not in ("failed_request", "failed_http", "failed_verification", "error")]

    print(f"Discovered {len(provinces)} province records. Crawling {len(targets)} province(s).")
    if args.resume:
        print("Mode: RESUME")
    elif args.retry_failed:
        print("Mode: RETRY-FAILED")
    else:
        print("Mode: NORMAL")

    try:
        with sync_playwright() as pw:
            for index, province in enumerate(targets, start=1):
                code = province["departmentCode"]
                name = province["departmentName"]
                print(f"[{index}/{len(targets)}] {code} - {name}")
                print(f"    rootDepartmentId={province['departmentId']}")

                previous = checkpoint["items"].get(code, {})
                raw_path = raw_dir / f"{code}.json"

                if args.resume and previous.get("status") == "verified" and raw_path.exists() and not args.force:
                    print("    already verified; skipping")
                    continue

                if not args.force and previous.get("status") == "verified" and raw_path.exists():
                    print("    verified RAW exists; skipping (use --force to recrawl)")
                    continue

                success = False
                last_result: dict[str, Any] = {}
                for attempt in range(1, args.retries_per_province + 2):
                    print(f"    attempt {attempt}/{args.retries_per_province + 1}")
                    try:
                        success, result = crawl_one_province(
                            pw,
                            province,
                            args.year,
                            headed=args.headed,
                            timeout_ms=args.timeout,
                            ui_first=args.ui_first,
                        )
                        last_result = result
                        if success:
                            break
                    except KeyboardInterrupt:
                        raise
                    except Exception as exc:
                        last_result = {"status": "error", "error": str(exc)}
                    if not success and attempt <= args.retries_per_province:
                        wait = min(30.0, args.delay_max * attempt)
                        print(f"    retrying province after {wait:.1f}s...")
                        time.sleep(wait)

                if success:
                    text = last_result.pop("rawText")
                    raw_path.write_text(text, encoding="utf-8")
                    last_result["departmentId"] = province["departmentId"]
                    last_result["departmentName"] = name
                    last_result["departmentCode"] = code
                    last_result["rootDepartmentId"] = province["departmentId"]
                    checkpoint["items"][code] = last_result
                    print(
                        f"    VERIFIED | evaluation={last_result['evaluationCount']} | "
                        f"agency={last_result['agencyCount']} | commune={last_result['communeCount']} | method={last_result.get('method')}"
                    )
                else:
                    checkpoint["items"][code] = {
                        "departmentId": province["departmentId"],
                        "departmentName": name,
                        "departmentCode": code,
                        "rootDepartmentId": province["departmentId"],
                        **last_result,
                    }
                    print(f"    FAILED | {last_result.get('error', last_result.get('status', 'unknown'))}", file=sys.stderr)

                catalog_summary = rebuild_catalog(args.year, provinces, raw_dir, catalog_dir, checkpoint)
                save_checkpoint(checkpoint_path, checkpoint)
                write_manifest(args.year, ENDPOINT, checkpoint, catalog_summary, catalog_dir)

                wait = random.uniform(args.delay_min, args.delay_max)
                print(f"    waiting {wait:.1f}s before next province...")
                time.sleep(wait)
    except KeyboardInterrupt:
        catalog_summary = rebuild_catalog(args.year, provinces, raw_dir, catalog_dir, checkpoint)
        save_checkpoint(checkpoint_path, checkpoint)
        write_manifest(args.year, ENDPOINT, checkpoint, catalog_summary, catalog_dir)
        print("\nStopped by user. Checkpoint saved; resume is safe.")
        return 130

    catalog_summary = rebuild_catalog(args.year, provinces, raw_dir, catalog_dir, checkpoint)
    save_checkpoint(checkpoint_path, checkpoint)
    write_manifest(args.year, ENDPOINT, checkpoint, catalog_summary, catalog_dir)

    verified = sum(1 for item in checkpoint["items"].values() if item.get("status") == "verified")
    failed = len(provinces) - verified
    print("\nSUMMARY")
    print(f"  requested provinces : {len(targets)}")
    print(f"  verified provinces  : {verified}")
    print(f"  failed/not verified  : {failed}")
    print(f"  agencies            : {catalog_summary['agencies']}")
    print(f"  communes            : {catalog_summary['communes']}")
    print(f"  checkpoint          : {checkpoint_path}")
    print(f"  raw directory       : {raw_dir}")
    print(f"  catalog directory   : {catalog_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
