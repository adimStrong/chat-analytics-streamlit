@echo off
echo ============================================
echo Setting up Daily Sync Schedule (7:00 AM)
echo ============================================

:: Delete existing task if any
schtasks /delete /tn "Juan365 Daily Sync" /f >nul 2>&1

:: Create scheduled task for 7am daily
schtasks /create /tn "Juan365 Daily Sync" /tr "C:\Users\us\Desktop\chat_analytics_dashboard\run_daily_sync.bat" /sc daily /st 07:00 /ru "%USERNAME%" /rl HIGHEST

if %errorlevel% equ 0 (
    echo.
    echo ============================================
    echo SUCCESS! Daily sync scheduled for 7:00 AM
    echo ============================================
    echo.
    echo Task Name: Juan365 Daily Sync
    echo Schedule: Every day at 7:00 AM
    echo Script: run_daily_sync.bat
    echo.
    echo To verify, run: schtasks /query /tn "Juan365 Daily Sync"
) else (
    echo.
    echo ERROR: Failed to create scheduled task.
    echo Try running this as Administrator.
)

echo.
pause
