#!/usr/bin/env python3
"""
Bootstrap the DVCQG province catalog with one direct, read-only API request.

This is intentionally conservative:
- exactly one POST to the public service-results endpoint;
- no concurrency, proxy, retry loop, WAF bypass, or IP rotation;
- the response is saved as raw evidence;
- province/rootDepartmentId values are copied only from the observed response.

The national service-results response currently exposes the 34 province-level
records in data.evaluation. Those records have childGroup == null and codes
such as H01, H05, ... . This script does not hard-code the province list.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ENDPOINT = "https://dichvucong.gov.vn/api/v1/reporting/evaluation/service-results"
ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
CONFIG_DIR = DATA_DIR / "config"
DISCOVERY_DIR = DATA_DIR / "discovery"
PROVINCE_CODE_RE = re.compile(r"^H\d{2}$", re.I)
UUID_RE = re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def clean(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    value = " ".join(value.split()).strip()
    return value or None


def is_uuid(value: Any) -> bool:
    return isinstance(value, str) and bool(UUID_RE.fullmatch(value.strip()))


def sha256_json(value: Any) -> str:
    raw = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def save_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding="utf-8")


def fetch(payload: dict[str, Any], timeout: int) -> tuple[int, str, Any]:
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    request = urllib.request.Request(
        ENDPOINT,
        data=body,
        method="POST",
        headers={
            "Accept": "application/json",
            "Content-Type": "application/json",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
        },
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        raw = response.read()
        text = raw.decode("utf-8", errors="replace")
        content_type = response.headers.get("Content-Type", "")
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError as exc:
            raise RuntimeError(
                f"DVCQG returned non-JSON content: HTTP {response.status}, Content-Type={content_type}"
            ) from exc
        return response.status, content_type, parsed


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--year", type=int, default=2026)
    parser.add_argument("--timeout", type=int, default=45)
    args = parser.parse_args()

    request_payload = {
        "timeType": "year",
        "year": args.year,
        "departmentType": "ADMINISTRATIVE_UNIT",
    }

    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    run_dir = DISCOVERY_DIR / f"api-{timestamp}"
    run_dir.mkdir(parents=True, exist_ok=True)

    print(f"POST {ENDPOINT}")
    print(f"Request: {json.dumps(request_payload, ensure_ascii=False)}")

    try:
        status, content_type, response = fetch(request_payload, args.timeout)
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        print(f"HTTP ERROR {exc.code}: {body[:1000]}", file=sys.stderr)
        return 2
    except urllib.error.URLError as exc:
        print(f"NETWORK ERROR: {exc}", file=sys.stderr)
        return 3
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 4

    save_json(run_dir / "request.json", request_payload)
    save_json(run_dir / "response.json", response)

    data = response.get("data") if isinstance(response, dict) else None
    evaluation = data.get("evaluation") if isinstance(data, dict) else None
    if not isinstance(evaluation, list):
        print("ERROR: response.data.evaluation is missing or not a list", file=sys.stderr)
        return 5

    province_rows: list[dict[str, Any]] = []
    for row in evaluation:
        if not isinstance(row, dict):
            continue
        code = clean(row.get("departmentCode"))
        name = clean(row.get("departmentName"))
        did = row.get("departmentId")
        child_group = clean(row.get("childGroup"))
        if not (is_uuid(did) and code and name):
            continue
        if child_group is not None:
            continue
        if not PROVINCE_CODE_RE.fullmatch(code):
            continue
        province_rows.append({
            "departmentId": did,
            "departmentName": name,
            "departmentCode": code,
            "rootDepartmentId": did,
            "source": "national-service-results",
            "sourceUrl": ENDPOINT,
            "evidencePath": "$.data.evaluation",
        })

    # One row per province code; preserve first occurrence.
    dedup: dict[str, dict[str, Any]] = {}
    for row in province_rows:
        dedup.setdefault(row["departmentCode"], row)
    province_rows = sorted(dedup.values(), key=lambda r: r["departmentCode"])

    departments_output = {
        "schemaVersion": 1,
        "generatedAt": utc_now(),
        "source": ENDPOINT,
        "summary": {
            "total": len(province_rows),
            "agency": 0,
            "commune": 0,
            "unclassified": len(province_rows),
        },
        "agencies": [],
        "communes": [],
        "unclassified": province_rows,
    }
    provinces_output = {
        "schemaVersion": 1,
        "generatedAt": utc_now(),
        "source": ENDPOINT,
        "count": len(province_rows),
        "provinces": province_rows,
        "note": "Province records and rootDepartmentId are copied directly from the observed national service-results response.",
    }
    audit = {
        "schemaVersion": 1,
        "generatedAt": utc_now(),
        "endpoint": ENDPOINT,
        "request": request_payload,
        "httpStatus": status,
        "contentType": content_type,
        "responseSha256": sha256_json(response),
        "evaluationCount": len(evaluation),
        "provinceCount": len(province_rows),
        "safety": {
            "requestCount": 1,
            "parallelism": 1,
            "retryCount": 0,
            "proxyOrIpRotation": False,
        },
    }

    save_json(CONFIG_DIR / "provinces.json", provinces_output)
    save_json(CONFIG_DIR / "departments.json", departments_output)
    save_json(run_dir / "audit.json", audit)

    print(json.dumps({
        "httpStatus": status,
        "contentType": content_type,
        "evaluation": len(evaluation),
        "provinceCount": len(province_rows),
        "runDir": str(run_dir),
    }, ensure_ascii=False, indent=2))

    if status not in (200, 201):
        print(f"ERROR: unexpected HTTP status {status}", file=sys.stderr)
        return 6
    if len(province_rows) < 34:
        print(f"ERROR: expected at least 34 province records, found {len(province_rows)}", file=sys.stderr)
        return 7
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
