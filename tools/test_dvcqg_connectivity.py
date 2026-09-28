#!/usr/bin/env python3
"""Very small, read-only connectivity diagnostic for DVCQG.

No POST is made. The script checks DNS/HTTPS connectivity to the public host
and records timing/status only. It is intentionally lightweight and safe.
"""
from __future__ import annotations

import argparse
import random
import re
import socket
import ssl
import sys
import time
import urllib.error
import urllib.request

HOST = "dichvucong.gov.vn"
URL = "https://dichvucong.gov.vn/"


def generate_random_user_agent(
    device_type=None,
    browser_type=None,
    chrome_versions=(125, 138),
    firefox_versions=(120, 135),
) -> str:
    """Generate a randomized desktop or mobile User-Agent string across OS and browser types."""
    if not device_type:
        device_type = random.choice(["android", "ios", "windows", "ubuntu"])

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
        android_versions = ["10.0", "11.0", "12.0", "13.0", "14.0", "15.0", "16.0"]
        android_device = random.choice([
            "SM-G960F", "Pixel 5", "SM-A505F", "Pixel 4a", "Pixel 6 Pro", "SM-N975F",
            "SM-G973F", "Pixel 3", "SM-G980F", "Pixel 5a", "SM-G998B", "Pixel 4",
            "SM-G991B", "SM-G996B", "SM-F711B", "SM-F916B", "SM-G781B", "SM-N986B",
            "SM-N981B", "Pixel 2", "Pixel 2 XL", "Pixel 3 XL", "Pixel 4 XL",
            "Pixel 5 XL", "Pixel 6", "Pixel 6 XL", "Pixel 6a", "Pixel 7", "Pixel 7 Pro",
            "OnePlus 8", "OnePlus 8 Pro", "OnePlus 9", "OnePlus 9 Pro", "OnePlus Nord", "OnePlus Nord 2", "OnePlus Nord CE", "OnePlus 10", "OnePlus 10 Pro", "OnePlus 10T", "OnePlus 10T Pro",
            "Xiaomi Mi 9", "Xiaomi Mi 10", "Xiaomi Mi 11", "Xiaomi Redmi Note 8", "Xiaomi Redmi Note 9",
            "Huawei P30", "Huawei P40", "Huawei Mate 30", "Huawei Mate 40", "Sony Xperia 1",
            "Sony Xperia 5", "LG G8", "LG V50", "LG V60", "Nokia 8.3", "Nokia 9 PureView",
        ])
        android_version = random.choice(android_versions)
        if browser_type == "chrome":
            return f"Mozilla/5.0 (Linux; Android {android_version}; {android_device}) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{browser_version} Mobile Safari/537.36"
        elif browser_type == "firefox":
            return f"Mozilla/5.0 (Android {android_version}; Mobile; rv:{browser_version}.0) Gecko/{browser_version}.0 Firefox/{browser_version}.0"

    elif device_type == "ios":
        ios_versions = ["13.0", "14.0", "15.0", "16.0"]
        ios_device = random.choice([
            "iPhone X", "iPhone 11", "iPhone 12", "iPhone 13", "iPad Pro", "iPad Mini",
        ])
        ios_version = random.choice(ios_versions)
        if browser_type == "chrome":
            return f"Mozilla/5.0 (iPhone; CPU iPhone OS {ios_version.replace('.', '_')} like Mac OS X) AppleWebKit/537.36 (KHTML, like Gecko) CriOS/{browser_version} Mobile/15E148 Safari/604.1"
        elif browser_type == "firefox":
            return f"Mozilla/5.0 (iPhone; CPU iPhone OS {ios_version.replace('.', '_')} like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) FxiOS/{browser_version}.0 Mobile/15E148 Safari/605.1.15"

    elif device_type == "windows":
        windows_versions = ["10.0", "11.0"]
        windows_version = random.choice(windows_versions)
        if browser_type == "chrome":
            return f"Mozilla/5.0 (Windows NT {windows_version}; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{browser_version} Safari/537.36"
        elif browser_type == "firefox":
            return f"Mozilla/5.0 (Windows NT {windows_version}; Win64; x64; rv:{browser_version}.0) Gecko/{browser_version}.0 Firefox/{browser_version}.0"

    elif device_type == "ubuntu":
        ubuntu_versions = ["20.04", "22.04"]
        ubuntu_version = random.choice(ubuntu_versions)
        if browser_type == "chrome":
            return f"Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:94.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{browser_version} Safari/537.36"
        elif browser_type == "firefox":
            return f"Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:{browser_version}.0) Gecko/{browser_version}.0 Firefox/{browser_version}.0"

    return "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"


# Backward-compatible module-level USER_AGENT property
USER_AGENT = generate_random_user_agent()


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

    ua = generate_random_user_agent()
    print(f"Generated Random User-Agent: {ua}")
    print(f"HTTPS GET {URL}")
    t1 = time.monotonic()

    headers = {
        "User-Agent": ua,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
    }
    if "Chrome" in ua or "CriOS" in ua:
        ver_match = re.search(r"(?:Chrome|CriOS)/(\d+)", ua)
        ver = ver_match.group(1) if ver_match else "125"
        headers["Sec-Ch-Ua"] = f'"Chromium";v="{ver}", "Google Chrome";v="{ver}", "Not-A.Brand";v="99"'
        is_mobile = "Mobile" in ua or "Android" in ua or "iPhone" in ua
        headers["Sec-Ch-Ua-Mobile"] = "?1" if is_mobile else "?0"
        if "Windows" in ua:
            headers["Sec-Ch-Ua-Platform"] = '"Windows"'
        elif "Android" in ua:
            headers["Sec-Ch-Ua-Platform"] = '"Android"'
        elif "iPhone" in ua or "iPad" in ua:
            headers["Sec-Ch-Ua-Platform"] = '"iOS"'
        else:
            headers["Sec-Ch-Ua-Platform"] = '"Linux"'

    request = urllib.request.Request(
        URL,
        method="GET",
        headers=headers,
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
            print("WARNING: DVCQG connectivity test encountered an error; continuing execution.")
            return 0
        return 3
    print(f"HTTPS elapsed: {time.monotonic() - t1:.2f}s")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
