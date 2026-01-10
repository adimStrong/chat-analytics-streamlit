@echo off
REM ============================================
REM Update Data - Pull latest from Facebook API
REM Features:
REM   - 10 parallel workers for fast sync
REM   - Rate limiting (180 API calls/min)
REM   - Skip unchanged conversations
REM   - First run: 7 days, subsequent: 2 days
REM ============================================

echo.
echo ============================================
echo   Chat Analytics - Data Update
echo ============================================
echo   Features:
echo   - 10 parallel workers
echo   - Smart sync (skip unchanged)
echo   - Rate limiting enabled
echo ============================================
echo.

REM Change to script directory
cd /d "%~dp0"

REM Check if Python is available
python --version >nul 2>&1
if errorlevel 1 (
    echo ERROR: Python is not installed or not in PATH
    pause
    exit /b 1
)

REM Check if tokens.json exists
if not exist tokens.json (
    echo ERROR: tokens.json not found!
    echo Please create tokens.json with your page access tokens.
    pause
    exit /b 1
)

REM Run the sync script
echo Starting data sync...
echo.
python sync_data.py

if errorlevel 1 (
    echo.
    echo ERROR: Data sync failed!
    pause
    exit /b 1
)

echo.
echo ============================================
echo   Data update complete!
echo ============================================
echo.
pause
