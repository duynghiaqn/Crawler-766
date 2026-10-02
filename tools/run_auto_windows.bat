@echo off
chcp 65001 > nul
setlocal enabledelayedexpansion

:: ==============================================================================
:: Runner Script cho Windows Task Scheduler & Chạy thủ công - Crawler 766
:: Tự động thực thi crawler, đồng bộ git và tạo lại cache jsDelivr CDN
:: ==============================================================================

set "SCRIPT_DIR=%~dp0"
cd /d "%SCRIPT_DIR%.."
set "WORKSPACE_DIR=%CD%"

:: Thiết lập mã hóa UTF-8 chuẩn cho môi trường Python trên Windows
set "PYTHONIOENCODING=utf-8"
set "PYTHONUTF8=1"

:: Hiển thị trợ giúp nếu truyền cờ -h hoặc --help
if "%~1"=="-h" goto show_help
if "%~1"=="--help" goto show_help
goto find_python

:show_help
echo ==============================================================================
echo 🚀 CRAWLER 766 - WINDOWS AUTO RUNNER
echo ==============================================================================
echo Sử dụng:
echo   run_auto_windows.bat [TÙY CHỌN]
echo.
echo Các tùy chọn phổ biến:
echo   (không tham số)     Chạy toàn bộ crawler, commit git, push và purge CDN cache
echo   --skip-test         Bỏ qua bước kiểm tra kết nối DVCQG
echo   --skip-detail       Chỉ crawl dữ liệu tổng hợp (bỏ qua chi tiết các chỉ tiêu)
echo   --only-gl           Chỉ crawl dữ liệu tỉnh Gia Lai
echo   --only-provinces    Chỉ crawl dữ liệu 34 Tỉnh / Thành phố
echo   --no-push           Chỉ crawl và lưu dữ liệu cục bộ (không đẩy lên git)
echo   --skip-purge        Không thực hiện xóa cache jsDelivr CDN
echo   -h, --help          Hiển thị trợ giúp này
echo ==============================================================================
exit /b 0

:find_python
:: Tìm Python executable phù hợp (venv cục bộ, py launcher hoặc hệ thống)
set "PYTHON_CMD="

if exist "%WORKSPACE_DIR%\.venv\Scripts\python.exe" (
    set "PYTHON_CMD=%WORKSPACE_DIR%\.venv\Scripts\python.exe"
) else if exist "%WORKSPACE_DIR%\venv\Scripts\python.exe" (
    set "PYTHON_CMD=%WORKSPACE_DIR%\venv\Scripts\python.exe"
) else if exist "%WORKSPACE_DIR%\env\Scripts\python.exe" (
    set "PYTHON_CMD=%WORKSPACE_DIR%\env\Scripts\python.exe"
)

if not defined PYTHON_CMD (
    for /f "tokens=*" %%i in ('where python 2^>nul') do (
        if not defined PYTHON_CMD set "PYTHON_CMD=%%i"
    )
)

if not defined PYTHON_CMD (
    for /f "tokens=*" %%i in ('where py 2^>nul') do (
        if not defined PYTHON_CMD set "PYTHON_CMD=%%i"
    )
)

if not defined PYTHON_CMD (
    echo ==============================================================================
    echo ❌ LỖI: Không tìm thấy Python trong hệ thống hoặc môi trường ảo!
    echo 💡 Vui lòng cài đặt Python (https://www.python.org/downloads/)
    echo    và nhớ tích chọn "Add Python to PATH" khi cài đặt.
    echo ==============================================================================
    if not defined SCHTASKS_ENV pause
    exit /b 1
)

echo ==============================================================================
echo 🤖 KHỞI ĐỘNG CRAWLER 766 AUTO RUNNER (WINDOWS)
echo 📅 Thời gian: %DATE% %TIME%
echo 🐍 Python   : !PYTHON_CMD!
echo 📂 Workspace: %WORKSPACE_DIR%
echo ==============================================================================
echo.

:: Thực thi auto_sync.py với các tham số truyền vào
"!PYTHON_CMD!" "%SCRIPT_DIR%auto_sync.py" --run-once --push %*

set "EXIT_CODE=!ERRORLEVEL!"
echo.
echo ==============================================================================
if !EXIT_CODE! equ 0 (
    echo ✅ TIẾN TRÌNH WINDOWS HOÀN TẤT THÀNH CÔNG!
) else (
    echo ❌ TIẾN TRÌNH WINDOWS THẤT BẠI VỚI MÃ LỖI: !EXIT_CODE!
)
echo ==============================================================================

:: Nếu chạy tương tác bằng cách click đúp chuột (không chạy từ Task Scheduler)
if not defined SCHTASKS_ENV (
    if !EXIT_CODE! neq 0 (
        echo.
        echo ⚠️ Đã xảy ra lỗi trong quá trình thực thi. Nhấn phím bất kỳ để đóng cửa sổ...
        pause > nul
    ) else (
        echo ⏳ Cửa sổ sẽ tự động đóng sau 5 giây...
        timeout /t 5 > nul 2>&1
    )
)

exit /b !EXIT_CODE!
