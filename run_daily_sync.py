"""
Master Daily Sync Script
Runs all sync scripts in correct order for T+1 reporting
Schedule this to run at 7am Philippine Time via Windows Task Scheduler
"""

import subprocess
import sys
import logging
from datetime import datetime, date
from pathlib import Path

# Setup logging
LOG_DIR = Path(__file__).parent / "logs"
LOG_DIR.mkdir(exist_ok=True)
log_file = LOG_DIR / f"sync_{date.today().strftime('%Y%m%d')}.log"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Scripts to run in order (script_cmd, description)
SCRIPTS = [
    ("sync_data.py", "Facebook Data Sync"),
    ("sync_schedule_gsheet.py", "Schedule Sync"),
    ("aggregate_daily_stats.py --days 3", "Daily Stats Aggregation"),
]


def run_script(script_cmd, description):
    """Run a script and return success status"""
    logger.info(f"Starting: {description}")
    start_time = datetime.now()

    try:
        result = subprocess.run(
            [sys.executable] + script_cmd.split(),
            cwd=Path(__file__).parent,
            capture_output=True,
            text=True,
            timeout=1800  # 30 minute timeout
        )

        duration = (datetime.now() - start_time).total_seconds()

        if result.returncode == 0:
            logger.info(f"Completed: {description} ({duration:.1f}s)")
            if result.stdout:
                # Log last few lines of output
                lines = result.stdout.strip().split('\n')
                for line in lines[-5:]:
                    logger.info(f"  {line}")
            return True
        else:
            logger.error(f"Failed: {description}")
            if result.stderr:
                logger.error(f"  Error: {result.stderr[:500]}")
            return False

    except subprocess.TimeoutExpired:
        logger.error(f"Timeout: {description} (exceeded 30 min)")
        return False
    except Exception as e:
        logger.error(f"Error running {description}: {e}")
        return False


def main():
    logger.info("=" * 60)
    logger.info("Starting Daily Sync - T+1 Report")
    logger.info(f"Date: {date.today().strftime('%Y-%m-%d')}")
    logger.info("=" * 60)

    start_time = datetime.now()
    results = {}

    for script_cmd, description in SCRIPTS:
        results[description] = run_script(script_cmd, description)

    total_duration = (datetime.now() - start_time).total_seconds()

    # Summary
    logger.info("=" * 60)
    logger.info("Sync Summary:")
    for desc, success in results.items():
        status = "OK" if success else "FAILED"
        logger.info(f"  [{status}] {desc}")
    logger.info(f"Total Duration: {total_duration:.1f}s ({total_duration/60:.1f} min)")
    logger.info("=" * 60)

    # Return exit code based on all scripts succeeding
    all_success = all(results.values())
    if all_success:
        logger.info("Daily sync completed successfully!")
    else:
        logger.warning("Daily sync completed with errors - check logs")

    return 0 if all_success else 1


if __name__ == "__main__":
    sys.exit(main())
