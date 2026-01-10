@echo off
REM ============================================
REM Push Changes to Git Repository
REM ============================================

echo.
echo ============================================
echo   Chat Analytics - Git Push
echo ============================================
echo.

REM Change to script directory
cd /d "%~dp0"

REM Check if git is available
git --version >nul 2>&1
if errorlevel 1 (
    echo ERROR: Git is not installed or not in PATH
    pause
    exit /b 1
)

REM Show current status
echo Current git status:
echo.
git status --short

echo.
echo ============================================

REM Check if there are changes to commit
git diff-index --quiet HEAD -- 2>nul
if errorlevel 1 (
    echo Changes detected. Committing...
    echo.

    REM Add all changes
    git add -A

    REM Get current date/time for commit message
    for /f "tokens=2 delims==" %%I in ('wmic os get localdatetime /value') do set datetime=%%I
    set COMMIT_DATE=%datetime:~0,4%-%datetime:~4,2%-%datetime:~6,2% %datetime:~8,2%:%datetime:~10,2%

    REM Commit with timestamp
    git commit -m "Data sync update - %COMMIT_DATE%"

    if errorlevel 1 (
        echo ERROR: Commit failed!
        pause
        exit /b 1
    )
) else (
    echo No changes to commit.
)

echo.

REM Ask user if they want to push
set /p PUSH_CONFIRM="Push to remote? (y/n): "
if /i "%PUSH_CONFIRM%"=="y" (
    echo.
    echo Pushing to remote...
    git push

    if errorlevel 1 (
        echo ERROR: Push failed!
        pause
        exit /b 1
    )

    echo.
    echo Push complete!
) else (
    echo.
    echo Skipping push. Changes are committed locally.
)

echo.
echo ============================================
echo   Done!
echo ============================================
echo.
pause
