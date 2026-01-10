"""
Aggregate Daily Stats Script
Calculates daily message stats from the messages table and updates agent_daily_stats.
Attributes messages to agents based on page assignments and shift times.
"""

import os
import sys
import logging
from datetime import datetime, date, timedelta

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

try:
    import psycopg2
    from dotenv import load_dotenv
except ImportError as e:
    logger.error(f"Missing required package: {e}")
    sys.exit(1)

load_dotenv()


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


def aggregate_daily_stats(start_date: date = None, end_date: date = None):
    """
    Aggregate message stats from messages table into agent_daily_stats.
    Uses agent_page_assignments to attribute messages to agents based on page and shift.

    Args:
        start_date: Start date for aggregation (default: 7 days ago)
        end_date: End date for aggregation (default: today)
    """
    if start_date is None:
        start_date = date.today() - timedelta(days=7)
    if end_date is None:
        end_date = date.today()

    logger.info("=" * 50)
    logger.info("Starting Daily Stats Aggregation")
    logger.info(f"Date range: {start_date} to {end_date}")
    logger.info("=" * 50)

    conn = get_db_connection()
    cur = conn.cursor()

    # Aggregate messages by agent and date using page assignments and shift times
    # Shift times (Philippines timezone):
    #   Morning: 6:00 AM - 2:00 PM (6-14)
    #   Mid: 12:00 PM - 10:00 PM (12-22)
    #   Evening/GY: 10:00 PM - 6:00 AM (22-6)
    cur.execute("""
        WITH message_attribution AS (
            SELECT
                m.id as message_id,
                m.page_id,
                m.is_from_page,
                m.response_time_seconds,
                (m.message_time AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')::date as msg_date,
                EXTRACT(HOUR FROM (m.message_time AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')) as msg_hour,
                CASE
                    WHEN EXTRACT(HOUR FROM (m.message_time AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')) >= 6
                         AND EXTRACT(HOUR FROM (m.message_time AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')) < 14
                    THEN 'Morning'
                    WHEN EXTRACT(HOUR FROM (m.message_time AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')) >= 12
                         AND EXTRACT(HOUR FROM (m.message_time AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')) < 22
                    THEN 'Mid'
                    ELSE 'Evening'
                END as derived_shift
            FROM messages m
            WHERE (m.message_time AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')::date BETWEEN %s AND %s
        ),
        comment_attribution AS (
            SELECT
                c.page_id,
                c.reply_count,
                (c.comment_time AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')::date as comment_date,
                CASE
                    WHEN EXTRACT(HOUR FROM (c.comment_time AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')) >= 6
                         AND EXTRACT(HOUR FROM (c.comment_time AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')) < 14
                    THEN 'Morning'
                    WHEN EXTRACT(HOUR FROM (c.comment_time AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')) >= 12
                         AND EXTRACT(HOUR FROM (c.comment_time AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')) < 22
                    THEN 'Mid'
                    ELSE 'Evening'
                END as derived_shift
            FROM comments c
            WHERE (c.comment_time AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')::date BETWEEN %s AND %s
              AND c.reply_count > 0
        ),
        agent_messages AS (
            SELECT
                apa.agent_id,
                ma.msg_date,
                ma.is_from_page,
                ma.response_time_seconds
            FROM message_attribution ma
            JOIN agent_page_assignments apa ON ma.page_id = apa.page_id
            WHERE (
                (apa.shift = 'Morning' AND ma.derived_shift = 'Morning')
                OR (apa.shift = 'Mid' AND ma.derived_shift = 'Mid')
                OR (apa.shift IN ('Evening', 'GY') AND ma.derived_shift = 'Evening')
            )
        ),
        agent_comments AS (
            SELECT
                apa.agent_id,
                ca.comment_date,
                SUM(ca.reply_count) as comment_replies
            FROM comment_attribution ca
            JOIN agent_page_assignments apa ON ca.page_id = apa.page_id
            WHERE (
                (apa.shift = 'Morning' AND ca.derived_shift = 'Morning')
                OR (apa.shift = 'Mid' AND ca.derived_shift = 'Mid')
                OR (apa.shift IN ('Evening', 'GY') AND ca.derived_shift = 'Evening')
            )
            GROUP BY apa.agent_id, ca.comment_date
        ),
        message_stats AS (
            SELECT
                agent_id,
                msg_date,
                COUNT(*) FILTER (WHERE is_from_page = false) as messages_received,
                COUNT(*) FILTER (WHERE is_from_page = true) as messages_sent,
                AVG(CASE WHEN is_from_page = true AND response_time_seconds > 0 THEN response_time_seconds END) as avg_response_time
            FROM agent_messages
            GROUP BY agent_id, msg_date
        ),
        daily_stats AS (
            SELECT
                COALESCE(ms.agent_id, ac.agent_id) as agent_id,
                COALESCE(ms.msg_date, ac.comment_date) as stat_date,
                COALESCE(ms.messages_received, 0) as messages_received,
                COALESCE(ms.messages_sent, 0) as messages_sent,
                COALESCE(ms.avg_response_time, 0) as avg_response_time,
                COALESCE(ac.comment_replies, 0) as comment_replies
            FROM message_stats ms
            FULL OUTER JOIN agent_comments ac ON ms.agent_id = ac.agent_id AND ms.msg_date = ac.comment_date
        )
        SELECT
            a.id as agent_id,
            a.agent_name,
            ds.stat_date,
            ds.messages_received,
            ds.messages_sent,
            ds.avg_response_time,
            ds.comment_replies
        FROM daily_stats ds
        JOIN agents a ON ds.agent_id = a.id
        ORDER BY ds.stat_date, a.agent_name
    """, (start_date, end_date, start_date, end_date))

    stats = cur.fetchall()
    logger.info(f"Found {len(stats)} daily stat records to process")

    updated = 0
    inserted = 0
    errors = 0

    for row in stats:
        agent_id, agent_name, stat_date, msgs_recv, msgs_sent, avg_rt, comment_replies = row

        try:
            # Check if record exists and get schedule status
            cur.execute("""
                SELECT id, schedule_status FROM agent_daily_stats
                WHERE agent_id = %s AND date = %s
            """, (agent_id, stat_date))

            existing = cur.fetchone()

            if existing:
                record_id, schedule_status = existing

                # If agent is absent or off, set all activity to 0
                if schedule_status in ('absent', 'off'):
                    cur.execute("""
                        UPDATE agent_daily_stats
                        SET messages_received = 0,
                            messages_sent = 0,
                            avg_response_time_seconds = 0,
                            comment_replies = 0
                        WHERE agent_id = %s AND date = %s
                    """, (agent_id, stat_date))
                    logger.info(f"  {agent_name} on {stat_date}: {schedule_status} - set to 0")
                else:
                    # Update with actual stats
                    cur.execute("""
                        UPDATE agent_daily_stats
                        SET messages_received = %s,
                            messages_sent = %s,
                            avg_response_time_seconds = %s,
                            comment_replies = %s
                        WHERE agent_id = %s AND date = %s
                    """, (msgs_recv, msgs_sent, avg_rt or 0, comment_replies, agent_id, stat_date))
                    logger.info(f"  {agent_name} on {stat_date}: recv={msgs_recv}, sent={msgs_sent}, comments={comment_replies}")
                updated += 1
            else:
                # Insert new record (default to present if no schedule exists)
                cur.execute("""
                    INSERT INTO agent_daily_stats
                    (agent_id, date, messages_received, messages_sent, avg_response_time_seconds,
                     shift, schedule_status, duty_hours, comment_replies)
                    VALUES (%s, %s, %s, %s, %s, 'Morning', 'present', 8.0, %s)
                """, (agent_id, stat_date, msgs_recv, msgs_sent, avg_rt or 0, comment_replies))
                inserted += 1
                logger.info(f"  {agent_name} on {stat_date}: recv={msgs_recv}, sent={msgs_sent}, comments={comment_replies} (new)")

        except Exception as e:
            logger.error(f"Error updating {agent_name} on {stat_date}: {e}")
            errors += 1

    conn.commit()
    cur.close()
    conn.close()

    logger.info("=" * 50)
    logger.info("Aggregation Complete!")
    logger.info(f"  Updated: {updated}")
    logger.info(f"  Inserted: {inserted}")
    logger.info(f"  Errors: {errors}")
    logger.info("=" * 50)


def main():
    import argparse

    parser = argparse.ArgumentParser(description='Aggregate daily stats from messages')
    parser.add_argument('--start', type=str, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end', type=str, help='End date (YYYY-MM-DD)')
    parser.add_argument('--days', type=int, default=7, help='Days back from today (default: 7)')

    args = parser.parse_args()

    start_date = None
    end_date = None

    if args.start:
        start_date = datetime.strptime(args.start, '%Y-%m-%d').date()
    elif args.days:
        start_date = date.today() - timedelta(days=args.days)

    if args.end:
        end_date = datetime.strptime(args.end, '%Y-%m-%d').date()

    try:
        aggregate_daily_stats(start_date, end_date)
    except Exception as e:
        logger.error(f"Aggregation failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
