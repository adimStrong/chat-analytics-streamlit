@echo off
REM ============================================
REM Silent Data Sync - For scheduled tasks
REM Logs output to sync_log.txt
REM ============================================

REM Change to script directory
cd /d "%~dp0"

REM Set log file with date
set LOGFILE=sync_log_%date:~10,4%%date:~4,2%%date:~7,2%.txt

REM Run sync and log output
echo ============================================ >> %LOGFILE%
echo Sync started at %date% %time% >> %LOGFILE%
echo ============================================ >> %LOGFILE%

python sync_data.py >> %LOGFILE% 2>&1

echo. >> %LOGFILE%
echo Sync completed at %date% %time% >> %LOGFILE%
echo ============================================ >> %LOGFILE%
echo. >> %LOGFILE%
