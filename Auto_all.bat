@echo off
chcp 65001 > nul
setlocal enabledelayedexpansion

:: ==============================================================================
:: Auto_all.bat - Master Execution Script cho Windows - Crawler 766
:: Tự động thu thập toàn bộ dữ liệu (Gia Lai & 34 Tỉnh/TP), dọn dẹp dữ liệu cũ,
:: commit và push lên Git Repository, sau đó Purge và tạo lại cache jsDelivr CDN.
:: ==============================================================================

set "SCRIPT_DIR=%~dp0"
call "%SCRIPT_DIR%tools\run_auto_windows.bat" %*
exit /b %ERRORLEVEL%
