@echo off
chcp 65001 > nul
setlocal enabledelayedexpansion

:: ==============================================================================
:: Auto_all.bat - Master Execution Script cho Windows - Crawler 766
:: Tu dong thu thap toan bo du lieu, commit git, push va purge CDN cache.
:: Su dung tieng Viet khong dau de tuong thich 100%% CMD Windows.
:: ==============================================================================

:: Chuyen thu muc lam viec ve thu muc chua file bat (thu muc goc cua du an)
cd /d "%~dp0"
set "ROOT_DIR=%CD%"
set "TOOLS_DIR=%ROOT_DIR%\tools"

:: Thiet lap moi truong UTF-8 cho Python
set "PYTHONIOENCODING=utf-8"
set "PYTHONUTF8=1"

:: Kiem tra neu nguoi dung truyen co tro giup -h hoac --help
if "%~1"=="-h" goto show_help
if "%~1"=="--help" goto show_help
goto find_python

:show_help
echo ==============================================================================
echo   CRAWLER 766 - WINDOWS AUTO RUNNER (Auto_all.bat)
echo ==============================================================================
echo Su dung:
echo   Auto_all.bat [TUY CHON]
echo.
echo Cac tuy chon pho bien:
echo   (khong tham so)     Chay toan bo crawler, commit git, push va purge CDN
echo   --skip-test         Bo qua buoc kiem tra ket noi DVCQG
echo   --skip-detail       Chi crawl du lieu tong hop (bo qua chi tiet cac chi tieu)
echo   --only-gl           Chi crawl du lieu tinh Gia Lai
echo   --only-provinces    Chi crawl du lieu 34 Tinh / Thanh pho
echo   --no-push           Chi crawl va luu du lieu cuc bo (khong day len git)
echo   --skip-purge        Khong thuc hien xoa cache jsDelivr CDN
echo   -h, --help          Hien thi tro giup nay
echo ==============================================================================
echo.
pause
exit /b 0

:find_python
echo ==============================================================================
echo   CRAWLER 766 AUTO RUNNER - WINDOWS
echo   Thu muc lam viec: %ROOT_DIR%
echo ==============================================================================

set "PYTHON_EXE="
set "PYTHON_ARGS="

:: 1. Uu tien moi truong ao trong thu muc du an (.venv, venv, env)
if exist "%ROOT_DIR%\.venv\Scripts\python.exe" (
    set "PYTHON_EXE=%ROOT_DIR%\.venv\Scripts\python.exe"
) else if exist "%ROOT_DIR%\venv\Scripts\python.exe" (
    set "PYTHON_EXE=%ROOT_DIR%\venv\Scripts\python.exe"
) else if exist "%ROOT_DIR%\env\Scripts\python.exe" (
    set "PYTHON_EXE=%ROOT_DIR%\env\Scripts\python.exe"
)

:: 2. Kiem tra lenh py (Python Launcher cua Windows)
if not defined PYTHON_EXE (
    where py >nul 2>&1
    if !errorlevel! equ 0 (
        py -3 -c "import sys" >nul 2>&1
        if !errorlevel! equ 0 (
            set "PYTHON_EXE=py"
            set "PYTHON_ARGS=-3"
        )
    )
)

:: 3. Kiem tra python trong PATH (chay thu de loai bo alias WindowsApps bi loi)
if not defined PYTHON_EXE (
    where python >nul 2>&1
    if !errorlevel! equ 0 (
        python -c "import sys" >nul 2>&1
        if !errorlevel! equ 0 (
            set "PYTHON_EXE=python"
        )
    )
)

:: 4. Kiem tra python3 trong PATH
if not defined PYTHON_EXE (
    where python3 >nul 2>&1
    if !errorlevel! equ 0 (
        python3 -c "import sys" >nul 2>&1
        if !errorlevel! equ 0 (
            set "PYTHON_EXE=python3"
        )
    )
)

:: Neu van khong tim thay Python kha dung
if not defined PYTHON_EXE (
    echo.
    echo [LOI] Khong tim thay Python trong he thong hoac moi truong ao!
    echo Vui long cai dat Python 3 tai: https://www.python.org/downloads/
    echo LUU Y QUAN TRONG: Nho tich chon vao o [Add Python to PATH] khi cai dat.
    echo.
    pause
    exit /b 1
)

if defined PYTHON_ARGS (
    echo   Python su dung: !PYTHON_EXE! !PYTHON_ARGS!
) else (
    echo   Python su dung: !PYTHON_EXE!
)
echo.
echo Dang bat dau chay quy trinh thu thap va dong bo du lieu...
echo ==============================================================================
echo.

:: Kiem tra neu thu muc chua co .git (VD: giai nen tu file ZIP)
if not exist "%ROOT_DIR%\.git" (
    echo [THONG TIN] Phat hien ma nguon chua co thu muc .git (do tai file ZIP).
    echo He thong se tu dong khoi tao git va ket noi voi GitHub repository.
    echo.
)

:: Kiem tra file auto_sync.py co ton tai khong
if not exist "%TOOLS_DIR%\auto_sync.py" (
    echo.
    echo [LOI] Khong tim thay file %TOOLS_DIR%\auto_sync.py!
    echo Vui long kiem tra lai thu muc du an.
    echo.
    pause
    exit /b 1
)

:: Thuc thi script auto_sync.py voi day du tham so truyen vao
if defined PYTHON_ARGS (
    "%PYTHON_EXE%" %PYTHON_ARGS% "%TOOLS_DIR%\auto_sync.py" --run-once --push %*
) else (
    "%PYTHON_EXE%" "%TOOLS_DIR%\auto_sync.py" --run-once --push %*
)
set "EXIT_CODE=%ERRORLEVEL%"

echo.
echo ==============================================================================
if %EXIT_CODE% equ 0 (
    echo [THANH CONG] Tien trinh da hoan tat xuat sac!
) else (
    echo [THAT BAI] Tien trinh ket thuc voi ma loi: %EXIT_CODE%
)
echo ==============================================================================
echo.

:: Tam dung xem log neu chay truc tiep bang double click
if not defined SCHTASKS_ENV (
    if %EXIT_CODE% neq 0 (
        echo [THONG BAO] Da xay ra loi trong qua trinh thuc thi.
        echo Nhan phim bat ky de dong cua so...
        pause > nul
    ) else (
        echo [THONG BAO] Cua so se tu dong dong sau 5 giay...
        timeout /t 5 > nul 2>&1
    )
)

exit /b %EXIT_CODE%
