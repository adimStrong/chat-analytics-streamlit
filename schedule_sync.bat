@echo off
REM ============================================
REM Schedule Daily Data Sync - Windows Task Scheduler
REM ============================================

echo.
echo ============================================
echo   Chat Analytics - Schedule Daily Sync
echo ============================================
echo.

REM Get the directory of this script
set SCRIPT_DIR=%~dp0

echo This will create a Windows scheduled task to run data sync daily at 6:00 AM.
echo.
set /p CONFIRM="Do you want to continue? (y/n): "
if /i not "%CONFIRM%"=="y" (
    echo Cancelled.
    pause
    exit /b 0
)

echo.
echo Creating scheduled task...

REM Create the scheduled task
schtasks /create /tn "ChatAnalytics_DataSync" /tr "\"%SCRIPT_DIR%update_data_silent.bat\"" /sc daily /st 06:00 /f

if errorlevel 1 (
    echo.
    echo ERROR: Failed to create scheduled task.
    echo Try running this script as Administrator.
    pause
    exit /b 1
)

echo.
echo ============================================
echo   Scheduled task created successfully!
echo ============================================
echo.
echo Task Name: ChatAnalytics_DataSync
echo Schedule:  Daily at 6:00 AM
echo.
echo To modify the schedule, use Task Scheduler or run:
echo   schtasks /change /tn "ChatAnalytics_DataSync" /st HH:MM
echo.
echo To delete the task:
echo   schtasks /delete /tn "ChatAnalytics_DataSync" /f
echo.
pause
