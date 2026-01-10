"""
Facebook Graph API Data Sync Script
Fetches messages and conversations from Facebook Pages and updates the database

Features:
- Parallel processing (10 workers)
- Skip unchanged conversations
- Rate limiting to avoid API limits
- Smart sync: 7 days first run, 2 days for subsequent runs
"""

import os
import json
import requests
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timedelta
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed
import sys
import time
import threading

# Load environment variables
load_dotenv()

# Configuration
DATABASE_URL = os.getenv("DATABASE_URL")
FB_API_VERSION = "v18.0"
FB_BASE_URL = f"https://graph.facebook.com/{FB_API_VERSION}"

# Path to tokens file (7 core pages only)
TOKENS_FILE = os.path.join(os.path.dirname(__file__), "tokens.json")
SYNC_STATUS_FILE = os.path.join(os.path.dirname(__file__), "sync_status.json")

# Core pages to sync (matching config.py)
CORE_PAGES = [
    "Juan365",
    "JuanBingo",
    "Juan365 Cares",
    "Juan365 Live Stream",
    "Juan365 LiveStream",
    "JuanSports",
    "Juan365 Studios"
]

# Sync settings
MAX_WORKERS = 10  # Number of parallel page syncs
FIRST_RUN_DAYS = 7  # Days to fetch on first run
SUBSEQUENT_RUN_DAYS = 2  # Days to fetch on subsequent runs

# Rate limiting settings - Facebook limit is ~200 calls/user/hour, we can be faster
API_CALLS_PER_MINUTE = 600  # More realistic limit
MIN_DELAY_BETWEEN_CALLS = 0.1  # 100ms between calls (per worker)

# Thread-safe rate limiter - tracks calls but doesn't block heavily
class RateLimiter:
    def __init__(self, calls_per_minute):
        self.calls_per_minute = calls_per_minute
        self.min_interval = 60.0 / calls_per_minute
        self.lock = threading.Lock()
        self.call_count = 0
        self.start_time = time.time()

    def wait(self):
        # Light delay to prevent hammering
        time.sleep(MIN_DELAY_BETWEEN_CALLS)
        with self.lock:
            self.call_count += 1

    def get_stats(self):
        elapsed = time.time() - self.start_time
        rate = self.call_count / (elapsed / 60) if elapsed > 0 else 0
        return self.call_count, rate

# Global rate limiter
rate_limiter = RateLimiter(API_CALLS_PER_MINUTE)


def load_tokens():
    """Load tokens from JSON file"""
    if os.path.exists(TOKENS_FILE):
        with open(TOKENS_FILE, 'r') as f:
            return json.load(f)
    else:
        log(f"ERROR: Tokens file not found at {TOKENS_FILE}")
        return {}


def load_sync_status():
    """Load sync status from JSON file"""
    if os.path.exists(SYNC_STATUS_FILE):
        with open(SYNC_STATUS_FILE, 'r') as f:
            return json.load(f)
    return {}


def save_sync_status(status):
    """Save sync status to JSON file"""
    with open(SYNC_STATUS_FILE, 'w') as f:
        json.dump(status, f, indent=2, default=str)


def get_connection():
    """Get database connection"""
    return psycopg2.connect(DATABASE_URL)


def log(message):
    """Print timestamped log message"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")
    sys.stdout.flush()


def progress_bar(current, total, prefix='', length=40):
    """Display a progress bar (ASCII compatible for Windows)"""
    percent = current / total if total > 0 else 0
    filled = int(length * percent)
    bar = '#' * filled + '-' * (length - filled)
    print(f'\r  {prefix} |{bar}| {current}/{total} ({percent*100:.1f}%)', end='', flush=True)
    if current >= total:
        print()


def find_page_token(tokens, page_name):
    """Find token for a page by name (case-insensitive matching)"""
    page_name_lower = page_name.lower().strip()

    for token_page_name, token_data in tokens.items():
        if token_page_name.lower().strip() == page_name_lower:
            return token_data
        if token_data.get("page_name", "").lower().strip() == page_name_lower:
            return token_data

    variations = {
        "juancares": "Juan365 Cares",
        "juan365 cares": "Juan365 Cares",
        "juanbingo": "JuanBingo",
        "juansports": "JuanSports",
        "juan365 livestream": "Juan365 LiveStream",
        "juan365 live stream": "Juan365 Live Stream",
    }

    if page_name_lower in variations:
        return find_page_token(tokens, variations[page_name_lower])

    return None


def get_pages_from_db():
    """Get pages from database"""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT page_id, page_name FROM pages")
    pages = [(row[0], row[1]) for row in cur.fetchall()]
    cur.close()
    conn.close()
    return pages


def get_existing_conversations(page_id):
    """Get existing conversations with their last updated time"""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT conversation_id, updated_time
        FROM conversations
        WHERE page_id = %s
    """, (page_id,))
    existing = {row[0]: row[1] for row in cur.fetchall()}
    cur.close()
    conn.close()
    return existing


def fetch_conversations(page_id, access_token, since_date=None):
    """Fetch conversations from Facebook Graph API with rate limiting"""
    rate_limiter.wait()  # Rate limit

    url = f"{FB_BASE_URL}/{page_id}/conversations"
    params = {
        "access_token": access_token,
        "fields": "id,participants,updated_time,message_count",
        "limit": 100
    }

    if since_date:
        params["since"] = int(since_date.timestamp())

    conversations = []

    try:
        while url:
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()

            if "error" in data:
                return [], data['error'].get('message', 'Unknown error')

            conversations.extend(data.get("data", []))

            paging = data.get("paging", {})
            url = paging.get("next")
            if url:
                rate_limiter.wait()  # Rate limit for pagination
            params = {}

    except requests.exceptions.RequestException as e:
        return [], str(e)

    return conversations, None


def fetch_messages(conversation_id, access_token, since_date=None):
    """Fetch messages from a conversation with rate limiting"""
    rate_limiter.wait()  # Rate limit

    url = f"{FB_BASE_URL}/{conversation_id}/messages"
    params = {
        "access_token": access_token,
        "fields": "id,message,from,to,created_time",
        "limit": 100
    }

    if since_date:
        params["since"] = int(since_date.timestamp())

    messages = []

    try:
        while url:
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()

            if "error" in data:
                break

            messages.extend(data.get("data", []))

            paging = data.get("paging", {})
            url = paging.get("next")
            if url:
                rate_limiter.wait()  # Rate limit for pagination
            params = {}

    except requests.exceptions.RequestException:
        pass

    return messages


def upsert_conversations(conn, page_id, conversations):
    """Insert or update conversations in database"""
    if not conversations:
        return 0

    cur = conn.cursor()

    values = []
    for conv in conversations:
        participants = conv.get("participants", {}).get("data", [])
        participant_id = None
        participant_name = None
        for p in participants:
            if str(p.get("id")) != str(page_id):
                participant_id = p.get("id")
                participant_name = p.get("name")
                break

        values.append((
            conv["id"],
            page_id,
            participant_id,
            participant_name,
            conv.get("updated_time"),
            conv.get("message_count", 0)
        ))

    try:
        execute_values(cur, """
            INSERT INTO conversations (conversation_id, page_id, participant_id, participant_name, updated_time, message_count)
            VALUES %s
            ON CONFLICT (conversation_id)
            DO UPDATE SET
                updated_time = EXCLUDED.updated_time,
                message_count = EXCLUDED.message_count,
                participant_name = EXCLUDED.participant_name
        """, values)
        conn.commit()
    except Exception:
        conn.rollback()
        return 0

    cur.close()
    return len(values)


def upsert_messages(conn, page_id, conversation_id, messages):
    """Insert or update messages in database"""
    if not messages:
        return 0

    cur = conn.cursor()

    values = []
    for msg in messages:
        from_user = msg.get("from", {})
        is_from_page = str(from_user.get("id")) == str(page_id)

        values.append((
            msg["id"],
            conversation_id,
            page_id,
            from_user.get("id"),
            from_user.get("name"),
            msg.get("message", ""),
            msg.get("created_time"),
            is_from_page
        ))

    try:
        execute_values(cur, """
            INSERT INTO messages (message_id, conversation_id, page_id, sender_id, sender_name, message_text, message_time, is_from_page)
            VALUES %s
            ON CONFLICT (message_id)
            DO UPDATE SET
                message_text = EXCLUDED.message_text,
                message_time = EXCLUDED.message_time,
                sender_name = EXCLUDED.sender_name
        """, values)
        conn.commit()
    except Exception:
        conn.rollback()
        return 0

    cur.close()
    return len(values)


def calculate_response_times(conn, conversation_id):
    """Calculate response_time_seconds for page replies in a conversation"""
    cur = conn.cursor()

    # Get all messages in the conversation ordered by time
    cur.execute("""
        SELECT id, message_time, is_from_page
        FROM messages
        WHERE conversation_id = %s
        ORDER BY message_time ASC
    """, (conversation_id,))
    messages = cur.fetchall()

    if len(messages) < 2:
        cur.close()
        return

    last_user_msg_time = None
    updates = []

    for msg_id, msg_time, is_from_page in messages:
        if not is_from_page:
            # User message - record the time
            last_user_msg_time = msg_time
        elif is_from_page and last_user_msg_time is not None:
            # Page reply - calculate response time
            if msg_time and last_user_msg_time:
                response_seconds = (msg_time - last_user_msg_time).total_seconds()
                if response_seconds > 0:
                    updates.append((response_seconds, msg_id))

    # Update response times
    for response_seconds, msg_id in updates:
        cur.execute("""
            UPDATE messages SET response_time_seconds = %s WHERE id = %s
        """, (response_seconds, msg_id))

    conn.commit()
    cur.close()


def sync_page(page_id, page_name, access_token, since_date, last_sync_time=None):
    """Sync data for a single page with skip unchanged optimization"""
    result = {
        "page_name": page_name,
        "page_id": page_id,
        "conversations": 0,
        "conversations_skipped": 0,
        "messages": 0,
        "error": None,
        "sync_time": datetime.now().isoformat()
    }

    # Fetch conversations
    conversations, error = fetch_conversations(page_id, access_token, since_date)

    if error:
        result["error"] = error
        return result

    if not conversations:
        return result

    # Get existing conversations to check for changes
    existing_convos = get_existing_conversations(page_id)

    conn = get_connection()

    # Filter to only changed conversations
    changed_conversations = []
    for conv in conversations:
        conv_id = conv["id"]
        conv_updated = conv.get("updated_time")

        # Check if this conversation needs updating
        existing_updated = existing_convos.get(conv_id)

        if existing_updated is None:
            # New conversation
            changed_conversations.append(conv)
        elif conv_updated:
            # Parse timestamps and compare
            try:
                conv_dt = datetime.fromisoformat(conv_updated.replace('Z', '+00:00').replace('+0000', '+00:00'))
                if isinstance(existing_updated, str):
                    existing_dt = datetime.fromisoformat(existing_updated.replace('Z', '+00:00').replace('+0000', '+00:00'))
                else:
                    existing_dt = existing_updated.replace(tzinfo=None)
                    conv_dt = conv_dt.replace(tzinfo=None)

                if conv_dt > existing_dt:
                    changed_conversations.append(conv)
                else:
                    result["conversations_skipped"] += 1
            except:
                # If parsing fails, include it
                changed_conversations.append(conv)
        else:
            changed_conversations.append(conv)

    # Upsert all conversations (for metadata update)
    result["conversations"] = upsert_conversations(conn, page_id, conversations)

    # Only fetch messages for changed conversations
    for conv in changed_conversations:
        messages = fetch_messages(conv["id"], access_token, since_date)
        msg_count = upsert_messages(conn, page_id, conv["id"], messages)
        result["messages"] += msg_count

        # Calculate response times for this conversation
        if msg_count > 0:
            calculate_response_times(conn, conv["id"])

    conn.close()

    return result


def main():
    """Main sync function with parallel processing"""
    log("=" * 60)
    log("Facebook Data Sync - Starting")
    log("=" * 60)

    # Load tokens
    tokens = load_tokens()
    if not tokens:
        log("No tokens loaded. Exiting.")
        return

    log(f"Loaded {len(tokens)} page tokens")

    # Load sync status
    sync_status = load_sync_status()

    # Get pages from database
    db_pages = get_pages_from_db()
    log(f"Found {len(db_pages)} pages in database")

    # Prepare pages to sync
    pages_to_sync = []

    for page_id, page_name in db_pages:
        is_core = any(core.lower() in page_name.lower() or page_name.lower() in core.lower()
                      for core in CORE_PAGES)

        if not is_core:
            continue

        token_data = find_page_token(tokens, page_name)
        if not token_data:
            continue

        access_token = token_data.get("token")
        if not access_token:
            continue

        page_status = sync_status.get(page_id, {})
        last_sync = page_status.get("last_sync")

        if last_sync:
            days_back = SUBSEQUENT_RUN_DAYS
            log(f"  {page_name}: Last synced {last_sync[:10]}, fetching {days_back} days")
        else:
            days_back = FIRST_RUN_DAYS
            log(f"  {page_name}: First sync, fetching {days_back} days")

        since_date = datetime.now() - timedelta(days=days_back)

        pages_to_sync.append({
            "page_id": page_id,
            "page_name": page_name,
            "access_token": access_token,
            "since_date": since_date,
            "last_sync": last_sync
        })

    if not pages_to_sync:
        log("No pages to sync.")
        return

    log(f"\nSyncing {len(pages_to_sync)} pages with {MAX_WORKERS} parallel workers...")
    log(f"Rate limit: {API_CALLS_PER_MINUTE} API calls/minute")
    log("-" * 60)

    # Parallel sync
    results = []
    completed = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(
                sync_page,
                p["page_id"],
                p["page_name"],
                p["access_token"],
                p["since_date"],
                p["last_sync"]
            ): p for p in pages_to_sync
        }

        for future in as_completed(futures):
            completed += 1
            result = future.result()
            results.append(result)

            progress_bar(completed, len(pages_to_sync), prefix='Pages')

            if not result.get("error"):
                sync_status[result["page_id"]] = {
                    "last_sync": result["sync_time"],
                    "conversations": result["conversations"],
                    "messages": result["messages"]
                }

    # Save sync status
    save_sync_status(sync_status)

    # Get rate limiter stats
    api_calls, api_rate = rate_limiter.get_stats()

    # Summary
    log("\n" + "=" * 60)
    log("Sync Complete!")
    log("-" * 60)

    total_convs = 0
    total_msgs = 0
    total_skipped = 0
    errors = []

    for r in results:
        status = "[OK]" if not r.get("error") else "[ERR]"
        skipped = r.get('conversations_skipped', 0)
        log(f"  {status} {r['page_name']}: {r['conversations']} convos ({skipped} skipped), {r['messages']} msgs")
        total_convs += r["conversations"]
        total_msgs += r["messages"]
        total_skipped += skipped
        if r.get("error"):
            errors.append(f"{r['page_name']}: {r['error']}")

    log("-" * 60)
    log(f"Total: {total_convs} conversations, {total_msgs} messages")
    log(f"Skipped: {total_skipped} unchanged conversations")
    log(f"API calls: {api_calls} ({api_rate:.1f}/min)")

    if errors:
        log(f"\nErrors ({len(errors)}):")
        for e in errors:
            log(f"  - {e}")

    log("=" * 60)


def recalculate_all_response_times():
    """Recalculate response_time_seconds for all messages (one-time fix)"""
    log("=" * 60)
    log("Recalculating Response Times for All Messages")
    log("=" * 60)

    conn = get_connection()
    cur = conn.cursor()

    # Get all conversations
    cur.execute("SELECT DISTINCT conversation_id FROM messages ORDER BY conversation_id")
    conversations = [row[0] for row in cur.fetchall()]
    cur.close()

    log(f"Found {len(conversations)} conversations to process")

    processed = 0
    for conv_id in conversations:
        calculate_response_times(conn, conv_id)
        processed += 1
        if processed % 1000 == 0:
            log(f"  Processed {processed}/{len(conversations)} conversations...")

    conn.close()
    log(f"Done! Processed {processed} conversations")
    log("=" * 60)


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "--recalc-rt":
        recalculate_all_response_times()
    else:
        main()
