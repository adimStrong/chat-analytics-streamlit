"""
Google Sheets Schedule Sync Script
Syncs agent schedule data from Google Sheets to the database

Handles pivot table format:
- Row 1: Headers (SMA, PAGE, RD, TIME, DUTY, date1, date2, ...)
- Row 2+: Agent data with status per date column
"""

import os
import sys
import json
import logging
import re
from datetime import datetime, date, timedelta
from typing import Optional, Dict, List

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

try:
    import gspread
    from google.oauth2.service_account import Credentials
    import psycopg2
    from dotenv import load_dotenv
except ImportError as e:
    logger.error(f"Missing required package: {e}")
    logger.error("Install with: pip install gspread google-auth psycopg2-binary python-dotenv")
    sys.exit(1)

# Load environment variables
load_dotenv()


# ============================================
# CONFIGURATION
# ============================================
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets.readonly',
    'https://www.googleapis.com/auth/drive.readonly'
]

# Status value mapping
STATUS_MAPPING = {
    'present': 'present',
    'p': 'present',
    'on': 'present',
    'on duty': 'present',
    'absent': 'absent',
    'a': 'absent',
    'off': 'off',
    'leave': 'leave',
    'vl': 'leave',
    'sl': 'leave',
    'rd': 'off',
    'rest day': 'off',
    '': 'off'
}

# Shift mapping
SHIFT_MAPPING = {
    'morning': 'Morning',
    'am': 'Morning',
    'mid': 'Mid',
    'mid shift': 'Mid',
    'pm': 'Mid',
    'graveyard': 'GY',
    'gy': 'GY',
    'night': 'GY'
}


def get_credentials():
    """Get Google credentials from Streamlit secrets or environment."""
    # Try Streamlit secrets first
    try:
        import streamlit as st
        if hasattr(st, 'secrets') and 'gcp_service_account' in st.secrets:
            creds_dict = dict(st.secrets['gcp_service_account'])
            credentials = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
            logger.info("Using credentials from Streamlit secrets")
            return credentials
    except Exception as e:
        logger.debug(f"Streamlit secrets not available: {e}")

    # Try credentials.json file
    creds_file = os.path.join(os.path.dirname(__file__), 'credentials.json')
    if os.path.exists(creds_file):
        credentials = Credentials.from_service_account_file(creds_file, scopes=SCOPES)
        logger.info(f"Using credentials from {creds_file}")
        return credentials

    # Try environment variable
    creds_json = os.getenv('GOOGLE_CREDENTIALS_JSON')
    if creds_json:
        creds_dict = json.loads(creds_json)
        credentials = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
        logger.info("Using credentials from GOOGLE_CREDENTIALS_JSON env var")
        return credentials

    raise ValueError(
        "Google credentials not found. Please provide credentials via:\n"
        "  1. Streamlit secrets (gcp_service_account section)\n"
        "  2. credentials.json file in project root\n"
        "  3. GOOGLE_CREDENTIALS_JSON environment variable"
    )


def get_sheet_url():
    """Get Google Sheet URL from config."""
    try:
        import streamlit as st
        if hasattr(st, 'secrets') and 'SCHEDULE_SHEET_URL' in st.secrets:
            return st.secrets['SCHEDULE_SHEET_URL']
    except Exception:
        pass

    url = os.getenv('SCHEDULE_SHEET_URL')
    if url:
        return url

    raise ValueError("SCHEDULE_SHEET_URL not found")


def get_worksheet_name():
    """Get worksheet name from config."""
    try:
        import streamlit as st
        if hasattr(st, 'secrets') and 'SCHEDULE_WORKSHEET_NAME' in st.secrets:
            return st.secrets['SCHEDULE_WORKSHEET_NAME']
    except Exception:
        pass

    return os.getenv('SCHEDULE_WORKSHEET_NAME', 'Schedule')


def get_db_connection():
    """Get database connection."""
    try:
        import streamlit as st
        if hasattr(st, 'secrets') and 'DATABASE_URL' in st.secrets:
            return psycopg2.connect(st.secrets['DATABASE_URL'])
    except Exception:
        pass

    db_url = os.getenv('DATABASE_URL')
    if db_url:
        return psycopg2.connect(db_url)

    raise ValueError("DATABASE_URL not found")


def parse_date_from_header(date_str: str) -> Optional[date]:
    """Parse date string from header in various formats."""
    if not date_str or date_str.strip() == '':
        return None

    date_str = str(date_str).strip()

    # Try common formats
    formats = [
        '%d/%m/%Y',      # 01/01/2026 (DD/MM/YYYY)
        '%m/%d/%Y',      # 01/01/2026 (MM/DD/YYYY)
        '%Y-%m-%d',      # 2026-01-01
        '%d-%m-%Y',      # 01-01-2026
        '%B %d, %Y',     # January 1, 2026
        '%b %d, %Y',     # Jan 1, 2026
        '%d %B %Y',      # 1 January 2026
        '%d %b %Y',      # 1 Jan 2026
    ]

    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt).date()
        except ValueError:
            continue

    return None


def normalize_status(status_str: str) -> str:
    """Normalize schedule status value."""
    if not status_str:
        return 'off'
    status_lower = str(status_str).strip().lower()
    return STATUS_MAPPING.get(status_lower, 'off')


def normalize_shift(shift_str: str) -> str:
    """Normalize shift value."""
    if not shift_str:
        return 'Morning'
    shift_lower = str(shift_str).strip().lower()

    for key, value in SHIFT_MAPPING.items():
        if key in shift_lower:
            return value

    return 'Morning'


def parse_duty_hours(duty_str: str) -> float:
    """Parse duty hours from string like '06AM-15PM' or '8'."""
    if not duty_str:
        return 8.0

    duty_str = str(duty_str).strip().upper()

    # Try to parse time range like "06AM-15PM" or "13PM-22PM"
    match = re.match(r'(\d{1,2})(?:AM|PM)?[-–](\d{1,2})(?:AM|PM)?', duty_str)
    if match:
        start = int(match.group(1))
        end = int(match.group(2))
        # Handle 24-hour format
        if 'PM' in duty_str and start < 12:
            start += 12
        if end < start:
            end += 12
        hours = end - start
        if hours > 0:
            return float(hours)

    # Try direct number
    try:
        return float(duty_str)
    except ValueError:
        return 8.0


def sync_schedule(
    target_date: Optional[date] = None,
    days_ahead: int = 7,
    dry_run: bool = False
):
    """
    Sync schedule data from Google Sheets to database.
    Handles pivot table format with dates as columns.
    """
    logger.info("=" * 50)
    logger.info("Starting Google Sheets Schedule Sync")
    logger.info("=" * 50)

    # Get credentials and connect to Google Sheets
    try:
        credentials = get_credentials()
        gc = gspread.authorize(credentials)
        logger.info("Successfully authenticated with Google")
    except Exception as e:
        logger.error(f"Failed to authenticate with Google: {e}")
        raise

    # Open the spreadsheet
    try:
        sheet_url = get_sheet_url()
        spreadsheet = gc.open_by_url(sheet_url)
        worksheet_name = get_worksheet_name()
        worksheet = spreadsheet.worksheet(worksheet_name)
        logger.info(f"Opened worksheet: {worksheet_name}")
    except Exception as e:
        logger.error(f"Failed to open spreadsheet: {e}")
        raise

    # Get all data from the sheet
    try:
        all_data = worksheet.get_all_values()
        if len(all_data) < 3:
            logger.warning("Not enough data in worksheet")
            return

        logger.info(f"Found {len(all_data)} rows")
    except Exception as e:
        logger.error(f"Failed to read worksheet data: {e}")
        raise

    # Find header row (row with SMA, PAGE, etc.)
    header_row_idx = None
    for i, row in enumerate(all_data[:5]):
        if 'SMA' in [str(cell).upper().strip() for cell in row]:
            header_row_idx = i
            break

    if header_row_idx is None:
        logger.error("Could not find header row with 'SMA' column")
        return

    header = all_data[header_row_idx]
    data_rows = all_data[header_row_idx + 1:]
    logger.info(f"Header row: {header_row_idx + 1}, Data rows: {len(data_rows)}")

    # Parse date columns (columns after DUTY)
    date_columns: Dict[int, date] = {}
    duty_col_idx = None

    for i, cell in enumerate(header):
        cell_upper = str(cell).upper().strip()
        if cell_upper == 'DUTY':
            duty_col_idx = i
        elif duty_col_idx is not None and i > duty_col_idx:
            parsed_date = parse_date_from_header(cell)
            if parsed_date:
                date_columns[i] = parsed_date

    logger.info(f"Found {len(date_columns)} date columns")
    if date_columns:
        logger.info(f"Date range: {min(date_columns.values())} to {max(date_columns.values())}")

    # Determine date range to sync
    today = date.today()
    if target_date:
        sync_dates = {target_date}
    else:
        sync_dates = {today + timedelta(days=i) for i in range(-7, days_ahead + 1)}

    logger.info(f"Will sync dates: {min(sync_dates)} to {max(sync_dates)}")

    # Connect to database
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        logger.info("Connected to database")
    except Exception as e:
        logger.error(f"Failed to connect to database: {e}")
        raise

    # Get agent name to ID mapping
    cur.execute("SELECT id, agent_name FROM agents")
    agent_map = {row[1].lower(): row[0] for row in cur.fetchall()}
    logger.info(f"Found {len(agent_map)} agents in database")

    # Find column indices
    sma_col = None
    time_col = None

    for i, cell in enumerate(header):
        cell_upper = str(cell).upper().strip()
        if cell_upper == 'SMA':
            sma_col = i
        elif cell_upper == 'TIME':
            time_col = i

    if sma_col is None:
        logger.error("Could not find SMA column")
        return

    # Process data rows
    updates: List[Dict] = []
    current_agent = None
    current_shift = 'Morning'
    current_duty = 8.0
    skipped = 0
    errors = 0

    for row_idx, row in enumerate(data_rows, start=header_row_idx + 2):
        try:
            # Pad row if needed
            while len(row) < len(header):
                row.append('')

            # Get agent name (may be empty if continuation row)
            agent_name = str(row[sma_col]).strip() if sma_col < len(row) else ''

            if agent_name:
                current_agent = agent_name
                # Get shift and duty from this row
                if time_col is not None and time_col < len(row):
                    current_shift = normalize_shift(row[time_col])
                if duty_col_idx is not None and duty_col_idx < len(row):
                    current_duty = parse_duty_hours(row[duty_col_idx])

            if not current_agent:
                continue

            # Find agent ID
            agent_id = agent_map.get(current_agent.lower())
            if not agent_id:
                # Try partial match
                for db_name, db_id in agent_map.items():
                    if current_agent.lower() in db_name or db_name in current_agent.lower():
                        agent_id = db_id
                        break

            if not agent_id:
                if agent_name:  # Only log for first occurrence
                    logger.warning(f"Row {row_idx}: Agent not found in database: {current_agent}")
                continue

            # Process each date column
            for col_idx, col_date in date_columns.items():
                if col_date not in sync_dates:
                    continue

                if col_idx >= len(row):
                    continue

                status = normalize_status(row[col_idx])

                # Only add if there's actual status data
                if row[col_idx].strip():
                    updates.append({
                        'date': col_date,
                        'agent_id': agent_id,
                        'agent_name': current_agent,
                        'shift': current_shift,
                        'status': status,
                        'duty_hours': current_duty
                    })

        except Exception as e:
            logger.error(f"Row {row_idx}: Error processing - {e}")
            errors += 1

    # Deduplicate updates (keep last occurrence for each agent+date)
    unique_updates: Dict[tuple, Dict] = {}
    for update in updates:
        key = (update['agent_id'], update['date'])
        unique_updates[key] = update

    updates = list(unique_updates.values())
    logger.info(f"Processed: {len(updates)} unique updates, {skipped} skipped, {errors} errors")

    if dry_run:
        logger.info("DRY RUN - No changes will be made")
        for update in updates[:15]:
            logger.info(f"  Would update: {update['agent_name']} on {update['date']} -> {update['status']} ({update['shift']})")
        if len(updates) > 15:
            logger.info(f"  ... and {len(updates) - 15} more")
        return

    # Update database
    updated = 0
    inserted = 0

    for update in updates:
        try:
            # Check if record exists
            cur.execute("""
                SELECT id FROM agent_daily_stats
                WHERE agent_id = %s AND date = %s
            """, (update['agent_id'], update['date']))

            existing = cur.fetchone()

            if existing:
                # Update existing record
                cur.execute("""
                    UPDATE agent_daily_stats
                    SET shift = %s,
                        schedule_status = %s,
                        duty_hours = %s
                    WHERE agent_id = %s AND date = %s
                """, (
                    update['shift'],
                    update['status'],
                    update['duty_hours'],
                    update['agent_id'],
                    update['date']
                ))
                updated += 1
            else:
                # Insert new record
                cur.execute("""
                    INSERT INTO agent_daily_stats
                    (agent_id, date, shift, schedule_status, duty_hours,
                     messages_received, messages_sent, comment_replies, avg_response_time_seconds)
                    VALUES (%s, %s, %s, %s, %s, 0, 0, 0, 0)
                """, (
                    update['agent_id'],
                    update['date'],
                    update['shift'],
                    update['status'],
                    update['duty_hours']
                ))
                inserted += 1

        except Exception as e:
            logger.error(f"Database error for {update['agent_name']} on {update['date']}: {e}")
            errors += 1

    conn.commit()
    cur.close()
    conn.close()

    # Update sync status
    sync_status = {
        'last_schedule_sync': datetime.now().isoformat(),
        'updated': updated,
        'inserted': inserted,
        'errors': errors
    }

    status_file = os.path.join(os.path.dirname(__file__), 'sync_status.json')
    try:
        with open(status_file, 'r') as f:
            existing_status = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        existing_status = {}

    existing_status.update(sync_status)

    with open(status_file, 'w') as f:
        json.dump(existing_status, f, indent=2)

    logger.info("=" * 50)
    logger.info("Sync Complete!")
    logger.info(f"  Updated: {updated}")
    logger.info(f"  Inserted: {inserted}")
    logger.info(f"  Errors: {errors}")
    logger.info("=" * 50)


def main():
    """Main entry point for CLI usage."""
    import argparse

    parser = argparse.ArgumentParser(description='Sync schedule from Google Sheets')
    parser.add_argument('--date', type=str, help='Specific date to sync (YYYY-MM-DD)')
    parser.add_argument('--days', type=int, default=7, help='Days ahead to sync (default: 7)')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be synced without writing')

    args = parser.parse_args()

    target_date = None
    if args.date:
        try:
            target_date = datetime.strptime(args.date, '%Y-%m-%d').date()
        except ValueError:
            logger.error(f"Invalid date format: {args.date}. Use YYYY-MM-DD")
            sys.exit(1)

    try:
        sync_schedule(
            target_date=target_date,
            days_ahead=args.days,
            dry_run=args.dry_run
        )
    except Exception as e:
        logger.error(f"Sync failed: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
