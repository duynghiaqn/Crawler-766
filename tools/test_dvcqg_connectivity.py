#!/usr/bin/env python3
"""Very small, read-only connectivity diagnostic for DVCQG.

No POST is made. The script checks DNS/HTTPS connectivity to the public host
and records timing/status only. It is intentionally lightweight and safe.
"""
from __future__ import annotations

import argparse
import socket
import ssl
import sys
import time
import urllib.error
import urllib.request

HOST = "dichvucong.gov.vn"
URL = "https://dichvucong.gov.vn/"
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--allow-fail", action="store_true", help="Do not exit with non-zero code on network timeout")
    parser.add_argument("--timeout", type=int, default=15)
    args = parser.parse_args()

    print(f"DNS {HOST}")
    t0 = time.monotonic()
    try:
        infos = socket.getaddrinfo(HOST, 443, type=socket.SOCK_STREAM)
        addresses = sorted({item[4][0] for item in infos})
        print(f"DNS OK | {len(addresses)} address(es) | {', '.join(addresses[:10])}")
    except Exception as exc:
        print(f"DNS ERROR: {exc}")
        return 0 if args.allow_fail else 1
    print(f"DNS elapsed: {time.monotonic() - t0:.2f}s")

    print(f"HTTPS GET {URL}")
    t1 = time.monotonic()
    request = urllib.request.Request(
        URL,
        method="GET",
        headers={
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=args.timeout, context=ssl.create_default_context()) as response:
            sample = response.read(256)
            print(f"HTTPS OK | status={response.status} | content-type={response.headers.get('Content-Type','')} | bytes-read={len(sample)}")
    except urllib.error.HTTPError as exc:
        print(f"HTTPS REACHED HOST | HTTP {exc.code} | content-type={exc.headers.get('Content-Type','')}")
    except urllib.error.URLError as exc:
        print(f"HTTPS NETWORK ERROR / TIMEOUT: {exc}")
        if args.allow_fail:
            print("WARNING: DVCQG connectivity test timed out or was blocked by firewall; continuing execution.")
            return 0
        return 2
    except Exception as exc:
        print(f"HTTPS ERROR: {exc}")
        if args.allow_fail:
            return 0
        return 3
    print(f"HTTPS elapsed: {time.monotonic() - t1:.2f}s")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
