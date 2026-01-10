@echo off
echo ============================================
echo Daily Sync - T+1 Report
echo Started: %date% %time%
echo ============================================

cd /d C:\Users\us\Desktop\chat_analytics_dashboard

echo.
echo [1/3] Syncing Facebook Messages...
python sync_data.py

echo.
echo [2/3] Syncing Agent Schedules from Google Sheets...
python sync_schedule_gsheet.py

echo.
echo [3/3] Aggregating Daily Stats...
python aggregate_daily_stats.py --days 3

echo.
echo ============================================
echo Daily Sync Complete!
echo Finished: %date% %time%
echo ============================================
pause
