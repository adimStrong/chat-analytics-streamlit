@echo off
echo ============================================
echo FORCE SYNC ALL - Full Data Refresh
echo Started: %date% %time%
echo ============================================
echo.
echo WARNING: This will recalculate ALL response times
echo and aggregate stats for the last 30 days.
echo This may take 30-60 minutes.
echo.
pause

cd /d C:\Users\us\Desktop\chat_analytics_dashboard

echo.
echo [1/4] Pulling Latest Facebook Messages (7 days)...
python sync_data.py

echo.
echo [2/4] Recalculating ALL Response Times...
python sync_data.py --recalc-rt

echo.
echo [3/4] Syncing Agent Schedules from Google Sheets...
python sync_schedule_gsheet.py --days 30

echo.
echo [4/4] Aggregating Daily Stats (30 days)...
python aggregate_daily_stats.py --days 30

echo.
echo ============================================
echo FORCE SYNC ALL Complete!
echo Finished: %date% %time%
echo ============================================
pause
