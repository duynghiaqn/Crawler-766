@echo off
chcp 65001 > nul
setlocal enabledelayedexpansion

:: ==============================================================================
:: Runner Script cho Windows Task Scheduler & Chạy thủ công - Crawler 766
:: Tự động thực thi crawler & đồng bộ push git
:: ==============================================================================

set "SCRIPT_DIR=%~dp0"
cd /d "%SCRIPT_DIR%.."
set "WORKSPACE_DIR=%CD%"

:: Tìm Python executable phù hợp (venv, .venv hoặc hệ thống)
set "PYTHON_CMD=python"
if exist "%WORKSPACE_DIR%\.venv\Scripts\python.exe" (
    set "PYTHON_CMD=%WORKSPACE_DIR%\.venv\Scripts\python.exe"
) else if exist "%WORKSPACE_DIR%\venv\Scripts\python.exe" (
    set "PYTHON_CMD=%WORKSPACE_DIR%\venv\Scripts\python.exe"
)

echo ==============================================================================
echo 🤖 KHỞI ĐỘNG CRAWLER 766 AUTO RUNNER (WINDOWS)
echo 📅 Ngày chạy: %DATE% %TIME%
echo 🐍 Python: %PYTHON_CMD%
echo 📂 Workspace: %WORKSPACE_DIR%
echo ==============================================================================

"%PYTHON_CMD%" "%SCRIPT_DIR%auto_sync.py" --run-once --push %*

set "EXIT_CODE=%ERRORLEVEL%"
echo.
echo ==============================================================================
if %EXIT_CODE% equ 0 (
    echo ✅ TIẾN TRÌNH HOÀN TẤT THÀNH CÔNG!
) else (
    echo ❌ TIẾN TRÌNH THẤT BẠI VỚI MÃ LỖI: %EXIT_CODE%
)
echo ==============================================================================

:: Nếu chạy tương tác bằng cách click đúp chuột thì dừng lại để xem log
if "%~1"=="" (
    if not defined SCHTASKS_ENV (
        timeout /t 5 > nul 2>&1
    )
)

exit /b %EXIT_CODE%
