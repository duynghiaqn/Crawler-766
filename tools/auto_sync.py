#!/usr/bin/env python3
"""
tools/auto_sync.py - Hệ thống Tự Động Chạy Crawler & Đồng Bộ Push Code Lên Git Repository (Tương thích Windows/Linux/macOS)

Tính năng chính:
1. Chạy Auto Pipelines:
   - Kiểm tra kết nối Cổng DVCQG (test_dvcqg_connectivity.py)
   - Thu thập điểm số & xếp hạng Gia Lai (crawl_gl.py)
   - Thu thập chi tiết chỉ tiêu con Gia Lai (crawl_gl_detail.py)
   - Thu thập điểm số & xếp hạng tất cả Tỉnh/Thành phố (crawl_province.py)
   - Thu thập chi tiết chỉ tiêu con tất cả Tỉnh/Thành phố (crawl_province_detail.py)
   - Tự động dọn dẹp dữ liệu snapshot cũ quá N ngày (clean_data.py)
2. Tự Động Git Commit & Push:
   - Tự động stage dữ liệu thư mục data/
   - Kiểm tra thay đổi dữ liệu (diff)
   - Commit với thông điệp chuẩn kèm ngày giờ Việt Nam
   - Tự động Pull Rebase tránh xung đột và Push lên remote branch (kèm cơ chế Retry 3 lần)
3. Lập Lịch Trên Windows:
   - Tích hợp Windows Task Scheduler (schtasks.exe): --install-task, --remove-task, --status-task
   - Chế độ vòng lặp Python daemon (--loop) theo giờ cố định hàng ngày (--daily-time 18:00) hoặc chu kỳ (--interval-hours 6)
   - Chế độ chạy 1 lần ngay lập tức (--run-once)
4. Thông báo Telegram (Tùy chọn):
   - Gửi webhook báo cáo kết quả nếu có biến môi trường GAS_WEBHOOK_URL hoặc TELEGRAM_BOT_TOKEN
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import shutil
import subprocess
import sys
import time
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

# Đảm bảo xuất UTF-8 mượt mà trên Windows CMD / PowerShell
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

# Đường dẫn thư mục gốc và tools
ROOT_DIR = Path(__file__).resolve().parents[1]
TOOLS_DIR = ROOT_DIR / "tools"
DATA_DIR = ROOT_DIR / "data"

VN_TZ = timezone(timedelta(hours=7))

# Tích hợp công cụ Purge & Warm-up jsDelivr CDN Cache
try:
    from tools.purge_cache import purge_and_warmup_jsdelivr_cache
except ImportError:
    try:
        from purge_cache import purge_and_warmup_jsdelivr_cache
    except ImportError:
        def purge_and_warmup_jsdelivr_cache(*args: Any, **kwargs: Any) -> bool:
            return True


def load_env_file() -> None:
    """Tự động tải các biến môi trường từ .env hoặc env_config nếu có."""
    env_paths = [ROOT_DIR / ".env", ROOT_DIR / "env_config", ROOT_DIR / "env_exam"]
    loaded_from = None
    for p in env_paths:
        if p.exists() and p.is_file():
            try:
                for line in p.read_text(encoding="utf-8").splitlines():
                    line = line.strip()
                    if not line or line.startswith("#") or "=" not in line:
                        continue
                    k, v = line.split("=", 1)
                    k = k.strip()
                    v = v.strip().strip("'\"")
                    if k and k not in os.environ:
                        os.environ[k] = v
                loaded_from = p.name
                break
            except Exception as exc:
                print(f"⚠️ Cảnh báo đọc file env ({p.name}): {exc}")

    if loaded_from:
        # Nếu có GITHUB_TOKEN nhưng chưa có mask log thì thông báo đã nạp
        pass


# Tải biến môi trường ngay khi khởi động
load_env_file()


def get_vn_time_str(fmt: str = "%d/%m/%Y %H:%M:%S") -> str:
    """Lấy chuỗi thời gian hiện tại theo giờ Việt Nam (UTC+7)."""
    return datetime.now(timezone.utc).astimezone(VN_TZ).strftime(fmt)


def log_header(title: str, icon: str = "🚀") -> None:
    """In thanh tiêu đề định dạng đẹp."""
    sep = "=" * 78
    print(f"\n{sep}")
    print(f"{icon}  {title}")
    print(f"⏰  Thời gian: {get_vn_time_str()}")
    print(f"{sep}\n", flush=True)


def log_step(step_idx: int, total_steps: int, name: str, icon: str = "👉") -> None:
    """In bước thực thi."""
    print(f"\n------------------------------------------------------------------------------")
    print(f"{icon} [{step_idx}/{total_steps}] {name}")
    print(f"------------------------------------------------------------------------------", flush=True)


def run_command(
    cmd: list[str],
    cwd: Path = ROOT_DIR,
    check: bool = True,
    capture_output: bool = False,
    env: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    """Thực thi lệnh hệ thống và hiển thị log thời gian thực."""
    run_env = os.environ.copy()
    if env:
        run_env.update(env)
    # Đảm bảo python con cũng xuất UTF-8
    run_env["PYTHONIOENCODING"] = "utf-8"
    run_env["PYTHONUTF8"] = "1"

    if capture_output:
        return subprocess.run(
            cmd,
            cwd=str(cwd),
            check=check,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            env=run_env,
        )

    return subprocess.run(
        cmd,
        cwd=str(cwd),
        check=check,
        text=True,
        encoding="utf-8",
        errors="replace",
        env=run_env,
    )


# ==============================================================================
# 1. CRAWLER PIPELINE EXECUTION
# ==============================================================================
def execute_crawler_pipeline(args: argparse.Namespace) -> bool:
    """Thực thi toàn bộ các bước crawler tuần tự."""
    start_time = time.time()
    log_header("BẮT ĐẦU QUY TRÌNH MASTER AUTO CRAWLER", icon="🤖")

    common_args: list[str] = [
        "--time-type", str(args.time_type),
        "--year", str(args.year),
        "--clean-days", str(args.clean_days),
    ]
    if getattr(args, "period", None):
        common_args.extend(["--period", str(args.period)])

    steps_to_run: list[tuple[str, list[str], bool]] = []

    # Bước 0: Kiểm tra kết nối mạng tới DVCQG
    if not args.skip_test:
        test_script = TOOLS_DIR / "test_dvcqg_connectivity.py"
        if test_script.exists():
            steps_to_run.append((
                "Kiểm tra kết nối tới Cổng DVCQG (test_dvcqg_connectivity.py)",
                [sys.executable, str(test_script), "--allow-fail"],
                False,  # allow fail
            ))

    # Bước 1: Crawl Gia Lai tổng hợp
    if not args.only_provinces:
        gl_script = TOOLS_DIR / "crawl_gl.py"
        if gl_script.exists():
            steps_to_run.append((
                "Thu thập Điểm số & Xếp hạng UBND Tỉnh Gia Lai (crawl_gl.py)",
                [sys.executable, str(gl_script)] + common_args,
                True,
            ))

        # Bước 2: Crawl Gia Lai chi tiết chỉ tiêu con
        if not args.skip_detail:
            gl_detail_script = TOOLS_DIR / "crawl_gl_detail.py"
            if gl_detail_script.exists():
                steps_to_run.append((
                    "Thu thập Chi tiết Chỉ tiêu con Đơn vị Gia Lai (crawl_gl_detail.py)",
                    [sys.executable, str(gl_detail_script)] + common_args,
                    True,
                ))

    # Bước 3: Crawl Toàn quốc Tỉnh/Thành phố tổng hợp
    if not args.only_gl:
        prov_script = TOOLS_DIR / "crawl_province.py"
        if prov_script.exists():
            steps_to_run.append((
                "Thu thập Điểm số & Xếp hạng Tất cả Tỉnh/Thành phố (crawl_province.py)",
                [sys.executable, str(prov_script)] + common_args,
                True,
            ))

        # Bước 4: Crawl Toàn quốc Tỉnh/Thành phố chi tiết
        if not args.skip_detail:
            prov_detail_script = TOOLS_DIR / "crawl_province_detail.py"
            if prov_detail_script.exists():
                steps_to_run.append((
                    "Thu thập Chi tiết Chỉ tiêu con Tất cả Tỉnh/Thành phố (crawl_province_detail.py)",
                    [sys.executable, str(prov_detail_script)] + common_args,
                    True,
                ))

    # Bước 5: Tự động dọn dẹp snapshots cũ
    clean_script = TOOLS_DIR / "clean_data.py"
    if clean_script.exists():
        steps_to_run.append((
            f"Dọn dẹp snapshot dữ liệu cũ quá {args.clean_days} ngày (clean_data.py)",
            [sys.executable, str(clean_script), "--clean-days", str(args.clean_days)],
            False,
        ))

    total_steps = len(steps_to_run)
    pipeline_success = True

    for idx, (title, cmd, required) in enumerate(steps_to_run, start=1):
        log_step(idx, total_steps, title)
        try:
            run_command(cmd, cwd=ROOT_DIR, check=True)
            print(f"✅ Hoàn thành: {title}")
        except subprocess.CalledProcessError as exc:
            print(f"❌ Lỗi khi thực hiện: {title} (Exit code: {exc.returncode})", file=sys.stderr)
            if required:
                print("⛔ Dừng tiến trình do bước bắt buộc gặp lỗi.", file=sys.stderr)
                pipeline_success = False
                break
            else:
                print("⚠️ Bước này cho phép bỏ qua (optional), tiếp tục các bước tiếp theo...")
        except Exception as exc:
            print(f"❌ Ngoại lệ bất ngờ: {exc}", file=sys.stderr)
            if required:
                pipeline_success = False
                break

    elapsed = int(time.time() - start_time)
    print("\n" + "=" * 78)
    if pipeline_success:
        print(f"✅ HOÀN THÀNH TẤT CẢ CÁC BƯỚC CRAWLER! (Thời gian chạy: {elapsed}s)")
    else:
        print(f"❌ TIẾN TRÌNH CRAWLER GẶP LỖI! (Thời gian chạy: {elapsed}s)")
    print("=" * 78 + "\n", flush=True)

    return pipeline_success


# ==============================================================================
# 2. GIT COMMIT & PUSH AUTOMATION
# ==============================================================================
def check_git_installed() -> bool:
    """Kiểm tra máy đã cài đặt git hay chưa."""
    try:
        res = run_command(["git", "--version"], check=False, capture_output=True)
        return res.returncode == 0
    except Exception:
        return False


def get_current_git_branch() -> str:
    """Lấy tên branch hiện tại."""
    try:
        res = run_command(["git", "branch", "--show-current"], check=False, capture_output=True)
        branch = res.stdout.strip()
        if branch:
            return branch
    except Exception:
        pass
    return "main"


def git_commit_and_push(
    remote: str = "origin",
    branch: str | None = None,
    commit_msg: str | None = None,
    max_retries: int = 3,
) -> bool:
    """Tự động thêm dữ liệu thay đổi, tạo commit và đẩy code lên remote repo."""
    log_header("TỰ ĐỘNG GIT COMMIT & PUSH DỮ LIỆU", icon="📤")

    if not check_git_installed():
        print("⚠️ Không tìm thấy công cụ git trong hệ thống PATH. Bỏ qua bước git push.")
        return False

    target_remote = remote or os.environ.get("GIT_REMOTE_NAME", "origin")
    target_branch = branch or os.environ.get("GIT_BRANCH") or get_current_git_branch()
    print(f"🌿 Nhánh mục tiêu: {target_branch} | Remote: {target_remote}")

    # Đảm bảo có cấu hình user name & email nếu chưa có
    configured_name = os.environ.get("GIT_USER_NAME", "Crawler 766 Bot")
    configured_email = os.environ.get("GIT_USER_EMAIL", "crawler-bot@users.noreply.github.com")

    try:
        user_name_proc = run_command(["git", "config", "user.name"], check=False, capture_output=True)
        if not user_name_proc.stdout.strip():
            print(f"⚙️ Thiết lập cấu hình git user.name: \"{configured_name}\"...")
            run_command(["git", "config", "--local", "user.name", configured_name], check=False)

        user_email_proc = run_command(["git", "config", "user.email"], check=False, capture_output=True)
        if not user_email_proc.stdout.strip():
            print(f"⚙️ Thiết lập cấu hình git user.email: \"{configured_email}\"...")
            run_command(["git", "config", "--local", "user.email", configured_email], check=False)
    except Exception as exc:
        print(f"⚠️ Không thể kiểm tra git config: {exc}")

    # Kiểm tra Token xác thực GitHub để hỗ trợ push tự động không cần nhập mật khẩu
    gh_token = os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN")
    gh_repo = os.environ.get("GITHUB_REPOSITORY")
    push_remote_target = target_remote

    if gh_token and gh_repo:
        # Sử dụng URL xác thực với Personal Access Token (chuẩn x-access-token hỗ trợ cả github_pat_ và ghp_)
        push_remote_target = f"https://x-access-token:{gh_token}@github.com/{gh_repo}.git"
        token_type = "Fine-grained Token (1 Repo)" if gh_token.startswith("github_pat_") else "Classic Token"
        print(f"🔑 Đã phát hiện GITHUB_TOKEN ({token_type}), kích hoạt chế độ xác thực tự động tới {gh_repo}.")

    # Stage data directory
    print("📁 Đang stage thư mục data/...")
    run_command(["git", "add", "data/"], check=True)

    # Kiểm tra xem có thay đổi nào trong stage không
    staged_diff = run_command(["git", "diff", "--staged", "--quiet"], check=False)
    if staged_diff.returncode == 0:
        print("ℹ️ Không phát hiện dữ liệu mới nào thay đổi trong data/. Không cần tạo commit.")
        return True

    # Tạo commit message
    vn_now = get_vn_time_str()
    default_msg = f"📊 Auto Update DVCQG Data ({vn_now})"
    final_msg = commit_msg or default_msg

    print(f"📝 Đang commit: \"{final_msg}\"...")
    try:
        run_command(["git", "commit", "-m", final_msg], check=True)
    except subprocess.CalledProcessError as exc:
        print(f"❌ Không thể tạo git commit: {exc}", file=sys.stderr)
        return False

    # Pull rebase và push với retry
    print(f"🚀 Đang đẩy dữ liệu lên {target_remote}/{target_branch} (Tối đa {max_retries} lần thử)...")
    push_success = False

    for attempt in range(1, max_retries + 1):
        print(f"\n🔄 [Lần thử {attempt}/{max_retries}] Đồng bộ với remote và push...")
        try:
            # Pull rebase autostash để tích hợp thay đổi từ xa nếu có
            pull_res = run_command(
                ["git", "pull", "--rebase", "--autostash", "-X", "ours", push_remote_target, target_branch],
                check=False,
                capture_output=True,
            )
            if pull_res.returncode != 0:
                print(f"⚠️ Cảnh báo git pull rebase: {pull_res.stderr.strip() or pull_res.stdout.strip()}")

            push_res = run_command(
                ["git", "push", push_remote_target, target_branch],
                check=False,
                capture_output=True,
            )
            if push_res.returncode == 0:
                print(f"✅ ĐẨY DỮ LIỆU LÊN REPO THÀNH CÔNG! ({remote}/{target_branch})")
                push_success = True
                break
            else:
                print(f"⚠️ Push thất bại lần {attempt}: {push_res.stderr.strip() or push_res.stdout.strip()}")
                time.sleep(3)
        except Exception as exc:
            print(f"⚠️ Ngoại lệ trong lần thử {attempt}: {exc}")
            time.sleep(3)

    return push_success


# ==============================================================================
# 3. TELEGRAM NOTIFICATIONS (OPTIONAL)
# ==============================================================================
def send_telegram_notification(
    status: str,
    message: str,
) -> None:
    """Gửi thông báo qua Telegram Webhook / GAS Webhook nếu được cấu hình."""
    webhook_url = os.environ.get("GAS_WEBHOOK_URL")
    secret_key = os.environ.get("GAS_SECRET_KEY", "")

    if not webhook_url:
        return

    try:
        time_vn = get_vn_time_str()
        payload = {
            "secret_key": secret_key,
            "secret": secret_key,
            "status": status,
            "status_text": "THÀNH CÔNG" if status == "success" else "THẤT BẠI",
            "time": time_vn,
            "message": message,
            "text": message,
        }
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            webhook_url,
            data=data,
            headers={"Content-Type": "application/json", "User-Agent": "Crawler766-AutoSync/1.0"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            if resp.status == 200:
                print("📲 Đã gửi thông báo qua Telegram Webhook thành công.")
    except Exception as exc:
        print(f"⚠️ Không thể gửi thông báo Telegram: {exc}")


# ==============================================================================
# 4. WINDOWS TASK SCHEDULER (schtasks.exe) INTEGRATION
# ==============================================================================
def create_windows_launcher_bat(task_name: str) -> Path:
    """Tạo tệp launcher .bat chuẩn hóa trên Windows để gọi bởi Task Scheduler."""
    bat_path = TOOLS_DIR / "run_auto_windows.bat"
    python_exe = sys.executable

    # Tạo nội dung batch script an toàn tuyệt đối với dấu tiếng Việt và đường dẫn có khoảng trắng
    content = f"""@echo off
chcp 65001 > nul
setlocal enabledelayedexpansion

:: ==============================================================================
:: Runner Script cho Windows Task Scheduler - Crawler 766
:: Tự động thực thi crawler & đồng bộ push git
:: ==============================================================================

cd /d "{ROOT_DIR}"

set "PYTHON_EXE={python_exe}"

:: Kiểm tra nếu có môi trường ảo local .venv hoặc venv
if exist "{ROOT_DIR}\\.venv\\Scripts\\python.exe" (
    set "PYTHON_EXE={ROOT_DIR}\\.venv\\Scripts\\python.exe"
) else if exist "{ROOT_DIR}\\venv\\Scripts\\python.exe" (
    set "PYTHON_EXE={ROOT_DIR}\\venv\\Scripts\\python.exe"
)

echo ==============================================================================
echo [Crawler 766 Auto Runner - Windows]
echo Running with: %PYTHON_EXE%
echo Workdir: {ROOT_DIR}
echo ==============================================================================

"%PYTHON_EXE%" "{TOOLS_DIR / 'auto_sync.py'}" --run-once --push %*

set EXIT_CODE=%ERRORLEVEL%
if %EXIT_CODE% neq 0 (
    echo [ERROR] Pipeline failed with exit code: %EXIT_CODE%
)

exit /b %EXIT_CODE%
"""
    bat_path.write_text(content, encoding="utf-8")
    print(f"📄 Đã tạo tệp launcher Windows: {bat_path}")
    return bat_path


def install_windows_task(task_name: str, daily_time: str) -> bool:
    """Đăng ký tác vụ tự động vào Windows Task Scheduler."""
    if platform.system() != "Windows":
        print(f"❌ Tính năng Task Scheduler chỉ hỗ trợ trên hệ điều hành Windows! (Hiện tại: {platform.system()})")
        print("💡 Trên Linux/macOS, bạn có thể dùng crontab hoặc chế độ '--loop' của script.")
        return False

    bat_path = create_windows_launcher_bat(task_name)

    # Validate daily_time format HH:MM
    time_parts = daily_time.split(":")
    if len(time_parts) != 2 or not (time_parts[0].isdigit() and time_parts[1].isdigit()):
        print("❌ Định dạng giờ không hợp lệ. Vui lòng nhập định dạng HH:MM (Ví dụ: 18:00, 01:30)")
        return False

    formatted_time = f"{int(time_parts[0]):02d}:{int(time_parts[1]):02d}"

    print(f"\n⚙️ Đang đăng ký tác vụ Windows Task Scheduler: \"{task_name}\"...")
    print(f"⏰ Lịch chạy hàng ngày lúc: {formatted_time}")
    print(f"🎯 Lệnh thực thi: \"{bat_path}\"")

    # schtasks /Create /SC DAILY /TN <name> /TR <cmd> /ST <time> /F
    cmd = [
        "schtasks", "/Create",
        "/SC", "DAILY",
        "/TN", task_name,
        "/TR", f'"{bat_path}"',
        "/ST", formatted_time,
        "/F",  # Force overwrite
    ]

    try:
        res = subprocess.run(cmd, capture_output=True, text=True, encoding="cp850", errors="replace")
        if res.returncode == 0:
            print(f"\n🎉 ĐĂNG KÝ TÁC VỤ WINDOWS THÀNH CÔNG!")
            print(f"✅ Tên tác vụ: {task_name}")
            print(f"⏰ Giờ chạy: {formatted_time} mỗi ngày")
            print("💡 Máy tính Windows sẽ tự động kích hoạt crawler và push code lên GitHub vào giờ này.")
            return True
        else:
            print(f"❌ Đăng ký tác vụ thất bại: {res.stderr.strip() or res.stdout.strip()}")
            print("💡 Lưu ý: Nếu gặp lỗi quyền hạn (Access Denied), hãy mở PowerShell hoặc CMD với quyền 'Run as Administrator'.")
            return False
    except Exception as exc:
        print(f"❌ Lỗi khi thực thi schtasks: {exc}")
        return False


def remove_windows_task(task_name: str) -> bool:
    """Xóa tác vụ khỏi Windows Task Scheduler."""
    if platform.system() != "Windows":
        print(f"❌ Tính năng này chỉ hỗ trợ trên hệ điều hành Windows! (Hiện tại: {platform.system()})")
        return False

    print(f"🗑️ Đang gỡ bỏ tác vụ Windows Task Scheduler: \"{task_name}\"...")
    cmd = ["schtasks", "/Delete", "/TN", task_name, "/F"]
    try:
        res = subprocess.run(cmd, capture_output=True, text=True, encoding="cp850", errors="replace")
        if res.returncode == 0:
            print(f"✅ Đã xóa thành công tác vụ \"{task_name}\" khỏi Windows Task Scheduler.")
            return True
        else:
            print(f"⚠️ Kết quả từ schtasks: {res.stderr.strip() or res.stdout.strip()}")
            return False
    except Exception as exc:
        print(f"❌ Lỗi khi xóa tác vụ: {exc}")
        return False


def status_windows_task(task_name: str) -> None:
    """Tra cứu trạng thái của tác vụ trong Windows Task Scheduler."""
    if platform.system() != "Windows":
        print(f"❌ Tính năng này chỉ hỗ trợ trên Windows! (Hiện tại: {platform.system()})")
        return

    cmd = ["schtasks", "/Query", "/TN", task_name, "/FO", "LIST", "/V"]
    try:
        res = subprocess.run(cmd, capture_output=True, text=True, encoding="cp850", errors="replace")
        if res.returncode == 0:
            print(f"\n📊 THÔNG TIN TÁC VỤ \"{task_name}\":")
            print(res.stdout)
        else:
            print(f"ℹ️ Tác vụ \"{task_name}\" chưa được tạo hoặc không tồn tại.")
    except Exception as exc:
        print(f"❌ Lỗi khi truy vấn: {exc}")


# ==============================================================================
# 5. PYTHON SCHEDULER DAEMON LOOP (--loop)
# ==============================================================================
def parse_time_list(times_str: str) -> list[tuple[int, int]]:
    """Phân tích danh sách giờ chạy HH:MM (có thể phân tách bằng dấu phẩy)."""
    results: list[tuple[int, int]] = []
    for item in times_str.split(","):
        item = item.strip()
        if not item:
            continue
        parts = item.split(":")
        if len(parts) == 2 and parts[0].isdigit() and parts[1].isdigit():
            h, m = int(parts[0]), int(parts[1])
            if 0 <= h < 24 and 0 <= m < 60:
                results.append((h, m))
    return sorted(results)


def get_next_run_time(
    daily_times: list[tuple[int, int]] | None,
    interval_hours: float | None,
) -> datetime:
    """Tính toán thời điểm chạy tiếp theo theo giờ Việt Nam."""
    now_vn = datetime.now(timezone.utc).astimezone(VN_TZ)

    if interval_hours and interval_hours > 0:
        return now_vn + timedelta(hours=interval_hours)

    if daily_times:
        candidates: list[datetime] = []
        for h, m in daily_times:
            # Hôm nay lúc H:M
            target_today = now_vn.replace(hour=h, minute=m, second=0, microsecond=0)
            if target_today > now_vn:
                candidates.append(target_today)
            else:
                # Ngày mai lúc H:M
                target_tomorrow = target_today + timedelta(days=1)
                candidates.append(target_tomorrow)
        return min(candidates)

    # Mặc định: 6 tiếng sau
    return now_vn + timedelta(hours=6)


def run_scheduler_loop(args: argparse.Namespace) -> None:
    """Chạy vòng lặp lập lịch liên tục trong Python (Python Daemon Loop)."""
    log_header("KHỞI ĐỘNG CHẾ ĐỘ LẬP LỊCH LIÊN TỤC (PYTHON SCHEDULER LOOP)", icon="⏱️")

    daily_times = parse_time_list(args.daily_time) if args.daily_time else [(18, 0)]
    interval_hours = args.interval_hours

    if interval_hours:
        print(f"🔄 Chế độ: Định kỳ mỗi {interval_hours} giờ/lần.")
    else:
        times_repr = ", ".join([f"{h:02d}:{m:02d}" for h, m in daily_times])
        print(f"📅 Chế độ: Chạy hàng ngày vào các khung giờ: [{times_repr}] (Giờ Việt Nam UTC+7)")

    if args.push:
        print("📤 Tự động Git Push: BẬT (Sẽ tự động commit & push lên repo sau mỗi lần crawl thành công)")
    else:
        print("ℹ️ Tự động Git Push: TẮT (Chỉ crawl và lưu dữ liệu cục bộ)")

    print("\n👉 Bấm Ctrl + C bất cứ lúc nào để dừng lập lịch an toàn.\n")

    # Thực hiện 1 lần đầu tiên ngay lập tức nếu người dùng muốn
    if getattr(args, "run_immediately", False):
        print("🚀 Đang chạy chu kỳ đầu tiên ngay lập tức...")
        success = execute_crawler_pipeline(args)
        if success and args.push:
            git_commit_and_push(remote=args.remote, branch=args.branch, commit_msg=args.commit_msg)

    while True:
        try:
            next_run = get_next_run_time(daily_times, interval_hours)
            next_run_str = next_run.strftime("%d/%m/%Y %H:%M:%S")

            while True:
                now_vn = datetime.now(timezone.utc).astimezone(VN_TZ)
                remaining = (next_run - now_vn).total_seconds()
                if remaining <= 0:
                    break

                hours, rem = divmod(int(remaining), 3600)
                mins, secs = divmod(rem, 60)
                sys.stdout.write(f"\r⏳ Lần chạy tiếp theo: {next_run_str} (Còn lại: {hours:02d}h {mins:02d}m {secs:02d}s)  ")
                sys.stdout.flush()
                time.sleep(min(10, max(1, remaining)))

            print("\n")
            log_header("ĐẾN GIỜ LẬP LỊCH - BẮT ĐẦU THỰC THI", icon="🔔")
            crawl_ok = execute_crawler_pipeline(args)

            if crawl_ok:
                if args.push:
                    push_ok = git_commit_and_push(
                        remote=args.remote,
                        branch=args.branch,
                        commit_msg=args.commit_msg,
                    )
                    status_text = "THÀNH CÔNG (Đã Crawl & Push Git)" if push_ok else "Crawl XONG, Push Git gặp lỗi"
                    if push_ok and not args.skip_purge:
                        purge_and_warmup_jsdelivr_cache(
                            repo=os.environ.get("GITHUB_REPOSITORY", "duynghiaqn/Crawler-766"),
                            branch=args.branch or "main",
                        )
                else:
                    status_text = "THÀNH CÔNG (Crawl hoàn tất)"
                    if args.purge and not args.skip_purge:
                        purge_and_warmup_jsdelivr_cache(
                            repo=os.environ.get("GITHUB_REPOSITORY", "duynghiaqn/Crawler-766"),
                            branch=args.branch or "main",
                        )
                send_telegram_notification("success", f"✅ [Crawler 766 Auto] {status_text} lúc {get_vn_time_str()}")
            else:
                send_telegram_notification("failure", f"❌ [Crawler 766 Auto] Quá trình crawl gặp lỗi lúc {get_vn_time_str()}")

        except KeyboardInterrupt:
            print("\n\n🛑 Nhận tín hiệu ngắt từ bàn phím (Ctrl+C). Đang tắt bộ lập lịch an toàn. Tạm biệt!\n")
            break
        except Exception as exc:
            print(f"\n❌ Lỗi ngoài dự kiến trong vòng lặp scheduler: {exc}", file=sys.stderr)
            time.sleep(30)


# ==============================================================================
# 6. MAIN CLI ARGUMENTS PARSER
# ==============================================================================
def parse_arguments() -> argparse.Namespace:
    current_year = datetime.now().year

    parser = argparse.ArgumentParser(
        description="🚀 Master Auto Runner & Git Sync for Crawler 766 (Windows & Cross-platform)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ví dụ sử dụng trên Windows / macOS / Linux:
-------------------------------------------------------------------------------
1. Chạy 1 lần toàn bộ crawler và tự động push lên Git (Mặc định):
   python tools/auto_sync.py --run-once --push

2. Đăng ký tự động chạy hàng ngày lúc 18:00 trên Windows (Windows Task Scheduler):
   python tools/auto_sync.py --install-task --daily-time 18:00

3. Xem trạng thái tác vụ Windows Task Scheduler:
   python tools/auto_sync.py --status-task

4. Xóa tác vụ khỏi Windows Task Scheduler:
   python tools/auto_sync.py --remove-task

5. Chạy vòng lặp nền liên tục trong Python (Python Daemon Loop):
   python tools/auto_sync.py --loop --daily-time 18:00 --push
   python tools/auto_sync.py --loop --interval-hours 6 --push

6. Chạy chỉ cho Gia Lai (hoặc bỏ qua các chỉ tiêu con chi tiết để chạy siêu nhanh):
   python tools/auto_sync.py --run-once --only-gl --skip-detail --push
-------------------------------------------------------------------------------
        """,
    )

    # Chế độ chạy
    mode_group = parser.add_argument_group("Chế độ chạy")
    mode_group.add_argument(
        "--run-once",
        action="store_true",
        default=False,
        help="Chạy quy trình 1 lần ngay lập tức rồi kết thúc",
    )
    mode_group.add_argument(
        "--loop",
        action="store_true",
        default=False,
        help="Chạy vòng lặp lập lịch liên tục trong nền Python",
    )
    mode_group.add_argument(
        "--install-task",
        action="store_true",
        default=False,
        help="[Windows] Đăng ký tác vụ tự động vào Windows Task Scheduler",
    )
    mode_group.add_argument(
        "--remove-task",
        action="store_true",
        default=False,
        help="[Windows] Gỡ bỏ tác vụ khỏi Windows Task Scheduler",
    )
    mode_group.add_argument(
        "--status-task",
        action="store_true",
        default=False,
        help="[Windows] Kiểm tra trạng thái tác vụ trong Windows Task Scheduler",
    )
    mode_group.add_argument(
        "--task-name",
        type=str,
        default="Crawler766_AutoSync",
        help="Tên tác vụ trên Windows Task Scheduler (Mặc định: Crawler766_AutoSync)",
    )

    # Lập lịch
    sched_group = parser.add_argument_group("Cấu hình Lập lịch")
    sched_group.add_argument(
        "--daily-time",
        type=str,
        default="18:00",
        help="Giờ chạy hàng ngày (HH:MM), có thể nhập nhiều mốc cách nhau bằng dấu phẩy, VD: '06:00,18:00' (Mặc định: 18:00)",
    )
    sched_group.add_argument(
        "--interval-hours",
        type=float,
        default=None,
        help="Lặp lại định kỳ mỗi N giờ (Ví dụ: 6 hoặc 12)",
    )
    sched_group.add_argument(
        "--run-immediately",
        action="store_true",
        default=False,
        help="Khi chạy --loop, thực hiện ngay 1 chu kỳ đầu tiên trước khi vào trạng thái chờ",
    )

    # Tùy chọn Crawler
    crawl_group = parser.add_argument_group("Tùy chọn Crawler")
    crawl_group.add_argument(
        "--time-type",
        type=str,
        choices=["year", "month", "quarter"],
        default="year",
        help="Loại thời gian trích xuất: year (mặc định), month, quarter",
    )
    crawl_group.add_argument(
        "--year",
        type=int,
        default=current_year,
        help=f"Năm trích xuất (Mặc định: {current_year})",
    )
    crawl_group.add_argument(
        "--period",
        type=int,
        default=None,
        help="Kỳ trích xuất (Tháng 1-12 hoặc Quý 1-4)",
    )
    crawl_group.add_argument(
        "--clean-days",
        type=int,
        default=3,
        help="Số ngày tự động dọn dẹp snapshot cũ (Mặc định: 3 ngày)",
    )
    crawl_group.add_argument(
        "--skip-test",
        action="store_true",
        default=False,
        help="Bỏ qua bước kiểm tra kết nối Cổng DVCQG",
    )
    crawl_group.add_argument(
        "--skip-detail",
        action="store_true",
        default=False,
        help="Bỏ qua các bước crawl chi tiết sub-metrics (chỉ crawl index và xếp hạng)",
    )
    crawl_group.add_argument(
        "--only-gl",
        action="store_true",
        default=False,
        help="Chỉ crawl dữ liệu tỉnh Gia Lai",
    )
    crawl_group.add_argument(
        "--only-provinces",
        action="store_true",
        default=False,
        help="Chỉ crawl dữ liệu tất cả các Tỉnh/Thành phố",
    )

    # Tùy chọn Git Push
    git_group = parser.add_argument_group("Tùy chọn Git Push")
    git_group.add_argument(
        "--push",
        dest="push",
        action="store_true",
        default=True,
        help="Tự động commit và push dữ liệu lên git repo sau khi crawl (Mặc định: Bật)",
    )
    git_group.add_argument(
        "--no-push",
        dest="push",
        action="store_false",
        help="Tắt tự động git push, chỉ crawl và lưu dữ liệu cục bộ",
    )
    git_group.add_argument(
        "--remote",
        type=str,
        default="origin",
        help="Tên Git remote (Mặc định: origin)",
    )
    git_group.add_argument(
        "--branch",
        type=str,
        default=None,
        help="Tên Git branch (Mặc định: tự động phát hiện branch hiện tại, ví dụ: main)",
    )
    git_group.add_argument(
        "--commit-msg",
        type=str,
        default=None,
        help="Nội dung custom cho git commit message",
    )

    purge_group = parser.add_argument_group("Tùy chọn jsDelivr CDN Cache")
    purge_group.add_argument(
        "--purge",
        action="store_true",
        help="Kích hoạt gửi Purge Requests và tạo lại cache mới trên jsDelivr CDN",
    )
    purge_group.add_argument(
        "--skip-purge",
        action="store_true",
        help="Bỏ qua bước xóa cache và tạo lại cache jsDelivr CDN sau khi push git",
    )

    return parser.parse_args()


# ==============================================================================
# MAIN ENTRYPOINT
# ==============================================================================
def main() -> None:
    args = parse_arguments()

    # 1. Quản lý Windows Task Scheduler
    if args.install_task:
        success = install_windows_task(args.task_name, args.daily_time)
        sys.exit(0 if success else 1)

    if args.remove_task:
        success = remove_windows_task(args.task_name)
        sys.exit(0 if success else 1)

    if args.status_task:
        status_windows_task(args.task_name)
        sys.exit(0)

    # 2. Chế độ vòng lặp lập lịch liên tục (Python Daemon)
    if args.loop:
        run_scheduler_loop(args)
        return

    # 3. Mặc định: Chạy 1 lần (One-shot)
    success = execute_crawler_pipeline(args)

    if success and args.push:
        push_ok = git_commit_and_push(
            remote=args.remote,
            branch=args.branch,
            commit_msg=args.commit_msg,
        )
        if push_ok:
            if not args.skip_purge:
                purge_and_warmup_jsdelivr_cache(
                    repo=os.environ.get("GITHUB_REPOSITORY", "duynghiaqn/Crawler-766"),
                    branch=args.branch or "main",
                )
            send_telegram_notification("success", f"✅ [Crawler 766 Auto] Đã chạy xong, push dữ liệu & purge jsDelivr cache lúc {get_vn_time_str()}")
        else:
            send_telegram_notification("failure", f"⚠️ [Crawler 766 Auto] Crawl thành công nhưng Git Push thất bại lúc {get_vn_time_str()}")
            sys.exit(1)
    elif success and args.purge and not args.skip_purge:
        purge_and_warmup_jsdelivr_cache(
            repo=os.environ.get("GITHUB_REPOSITORY", "duynghiaqn/Crawler-766"),
            branch=args.branch or "main",
        )
        send_telegram_notification("success", f"✅ [Crawler 766 Auto] Hoàn tất crawl & purge jsDelivr cache lúc {get_vn_time_str()}")
    elif not success:
        send_telegram_notification("failure", f"❌ [Crawler 766 Auto] Crawl thất bại lúc {get_vn_time_str()}")
        sys.exit(1)


if __name__ == "__main__":
    main()
