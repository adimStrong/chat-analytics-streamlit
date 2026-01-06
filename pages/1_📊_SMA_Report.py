"""
SMA (Social Media Agent) Daily Performance Report
With comment links for verification and page breakdown
"""

import streamlit as st
import pandas as pd
import psycopg2
from datetime import date, timedelta
import plotly.express as px
import plotly.graph_objects as go

# Page config
st.set_page_config(
    page_title="SMA Performance Report",
    page_icon="👥",
    layout="wide"
)

# Database connection
DATABASE_URL = "postgresql://postgres:OQKZTvPIBcUUSUowYaFZFNisaAADLwzF@tramway.proxy.rlwy.net:28999/railway"

def get_connection():
    """Get fresh database connection"""
    return psycopg2.connect(DATABASE_URL)

def format_number(val):
    """Format number with comma separator"""
    if pd.isna(val):
        return "N/A"
    if isinstance(val, (int, float)):
        if val == int(val):
            return f"{int(val):,}"
        return f"{val:,.1f}"
    return val

def format_dataframe_numbers(df, exclude_cols=None):
    """Apply comma formatting to numeric columns in a dataframe"""
    if exclude_cols is None:
        exclude_cols = []

    df_formatted = df.copy()
    for col in df_formatted.columns:
        if col not in exclude_cols and df_formatted[col].dtype in ['int64', 'float64', 'int32', 'float32']:
            df_formatted[col] = df_formatted[col].apply(format_number)
    return df_formatted

# ============================================
# DATA FUNCTIONS
# ============================================

@st.cache_data(ttl=300)
def get_date_range():
    """Get available date range from messages/comments"""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT MIN(dt), MAX(dt) FROM (
            SELECT MIN(message_time)::date as dt FROM messages
            UNION ALL
            SELECT MAX(message_time)::date FROM messages
            UNION ALL
            SELECT MIN(comment_time)::date FROM comments
            UNION ALL
            SELECT MAX(comment_time)::date FROM comments
        ) t
    """)
    row = cur.fetchone()
    cur.close()
    conn.close()
    return row[0], row[1]

@st.cache_data(ttl=60)
def get_pages():
    """Get all pages"""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT page_id, page_name FROM pages ORDER BY page_name")
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return pd.DataFrame(rows, columns=['page_id', 'page_name'])

@st.cache_data(ttl=60)
def get_daily_summary(start_date, end_date):
    """Get daily summary across all pages"""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT
            (m.message_time AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')::date as date,
            COUNT(*) FILTER (WHERE m.is_from_page = false) as messages_received,
            COUNT(*) FILTER (WHERE m.is_from_page = true) as messages_sent
        FROM messages m
        WHERE (m.message_time AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')::date BETWEEN %s AND %s
        GROUP BY date
        ORDER BY date DESC
    """, (start_date, end_date))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return pd.DataFrame(rows, columns=['date', 'messages_received', 'messages_sent'])

@st.cache_data(ttl=60)
def get_page_breakdown(start_date, end_date):
    """Get breakdown by page"""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT
            p.page_name as page,
            COUNT(*) FILTER (WHERE m.is_from_page = false) as msg_recv,
            COUNT(*) FILTER (WHERE m.is_from_page = true) as msg_sent,
            CASE
                WHEN COUNT(*) FILTER (WHERE m.is_from_page = false) > 0
                THEN ROUND((100.0 * COUNT(*) FILTER (WHERE m.is_from_page = true) /
                    COUNT(*) FILTER (WHERE m.is_from_page = false))::numeric, 1)
                ELSE 0
            END as response_rate
        FROM messages m
        JOIN pages p ON m.page_id = p.page_id
        WHERE (m.message_time AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')::date BETWEEN %s AND %s
        GROUP BY p.page_name
        HAVING COUNT(*) > 0
        ORDER BY COUNT(*) DESC
    """, (start_date, end_date))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return pd.DataFrame(rows, columns=['Page', 'Msg Recv', 'Msg Sent', 'Response Rate %'])

@st.cache_data(ttl=60)
def get_comments_by_page(start_date, end_date):
    """Get comments breakdown by page"""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT
            p.page_name as page,
            COUNT(*) FILTER (WHERE c.author_id IS NULL OR c.author_id != c.page_id) as comments_recv,
            COUNT(*) FILTER (WHERE c.author_id IS NOT NULL AND c.author_id = c.page_id) as page_replies
        FROM comments c
        JOIN pages p ON c.page_id = p.page_id
        WHERE (c.comment_time AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')::date BETWEEN %s AND %s
        GROUP BY p.page_name
        HAVING COUNT(*) > 0
        ORDER BY COUNT(*) DESC
    """, (start_date, end_date))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return pd.DataFrame(rows, columns=['Page', 'Comments Recv', 'Page Replies'])

@st.cache_data(ttl=60)
def get_shift_breakdown(start_date, end_date):
    """Get breakdown by shift"""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        WITH msg_shifts AS (
            SELECT
                CASE
                    WHEN EXTRACT(HOUR FROM (message_time AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')) BETWEEN 6 AND 13 THEN 'Morning (6am-2pm)'
                    WHEN EXTRACT(HOUR FROM (message_time AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')) BETWEEN 14 AND 21 THEN 'Mid (2pm-10pm)'
                    ELSE 'Evening (10pm-6am)'
                END as shift,
                is_from_page
            FROM messages
            WHERE (message_time AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')::date BETWEEN %s AND %s
        )
        SELECT
            shift,
            COUNT(*) as total_messages,
            COUNT(*) FILTER (WHERE is_from_page = false) as received,
            COUNT(*) FILTER (WHERE is_from_page = true) as sent,
            CASE
                WHEN COUNT(*) FILTER (WHERE is_from_page = false) > 0
                THEN ROUND((100.0 * COUNT(*) FILTER (WHERE is_from_page = true) /
                    COUNT(*) FILTER (WHERE is_from_page = false))::numeric, 1)
                ELSE 0
            END as response_rate
        FROM msg_shifts
        GROUP BY shift
        ORDER BY
            CASE shift
                WHEN 'Morning (6am-2pm)' THEN 1
                WHEN 'Mid (2pm-10pm)' THEN 2
                ELSE 3
            END
    """, (start_date, end_date))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return pd.DataFrame(rows, columns=['Shift', 'Total Messages', 'Received', 'Sent', 'Response Rate %'])

@st.cache_data(ttl=60)
def get_page_replies_with_links(start_date, end_date, page_filter=None, limit=50):
    """Get page reply comments with Facebook links for verification"""
    conn = get_connection()
    cur = conn.cursor()

    if page_filter and page_filter != "All Pages":
        cur.execute("""
            SELECT
                p.page_name as page,
                (c.comment_time AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila') as time_pht,
                LEFT(c.comment_text, 100) as reply_text,
                c.post_id,
                c.comment_id,
                CONCAT('https://www.facebook.com/', c.post_id) as post_link
            FROM comments c
            JOIN pages p ON c.page_id = p.page_id
            WHERE c.author_id = c.page_id
              AND (c.comment_time AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')::date BETWEEN %s AND %s
              AND p.page_name = %s
            ORDER BY c.comment_time DESC
            LIMIT %s
        """, (start_date, end_date, page_filter, limit))
    else:
        cur.execute("""
            SELECT
                p.page_name as page,
                (c.comment_time AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila') as time_pht,
                LEFT(c.comment_text, 100) as reply_text,
                c.post_id,
                c.comment_id,
                CONCAT('https://www.facebook.com/', c.post_id) as post_link
            FROM comments c
            JOIN pages p ON c.page_id = p.page_id
            WHERE c.author_id = c.page_id
              AND (c.comment_time AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')::date BETWEEN %s AND %s
            ORDER BY c.comment_time DESC
            LIMIT %s
        """, (start_date, end_date, limit))

    rows = cur.fetchall()
    cur.close()
    conn.close()
    return pd.DataFrame(rows, columns=['Page', 'Time (PHT)', 'Reply Text', 'Post ID', 'Comment ID', 'Post Link'])

@st.cache_data(ttl=60)
def get_totals(start_date, end_date):
    """Get overall totals"""
    conn = get_connection()
    cur = conn.cursor()

    # Messages
    cur.execute("""
        SELECT
            COUNT(*) FILTER (WHERE is_from_page = false) as recv,
            COUNT(*) FILTER (WHERE is_from_page = true) as sent
        FROM messages
        WHERE (message_time AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')::date BETWEEN %s AND %s
    """, (start_date, end_date))
    msg_row = cur.fetchone()

    # Comments
    cur.execute("""
        SELECT
            COUNT(*) FILTER (WHERE author_id IS NULL OR author_id != page_id) as recv,
            COUNT(*) FILTER (WHERE author_id IS NOT NULL AND author_id = page_id) as replies
        FROM comments
        WHERE (comment_time AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')::date BETWEEN %s AND %s
    """, (start_date, end_date))
    cmt_row = cur.fetchone()

    # Sessions - avg response time and unique conversations
    cur.execute("""
        SELECT
            COUNT(DISTINCT conversation_id) as unique_convos,
            AVG(avg_response_time_seconds) as avg_response_time
        FROM sessions
        WHERE (session_start AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')::date BETWEEN %s AND %s
          AND avg_response_time_seconds IS NOT NULL
          AND avg_response_time_seconds > 0
    """, (start_date, end_date))
    session_row = cur.fetchone()

    cur.close()
    conn.close()

    return {
        'msg_recv': msg_row[0] or 0,
        'msg_sent': msg_row[1] or 0,
        'cmt_recv': cmt_row[0] or 0,
        'cmt_reply': cmt_row[1] or 0,
        'unique_convos': session_row[0] or 0,
        'avg_response_time': session_row[1] or 0
    }

@st.cache_data(ttl=60)
def get_shift_stats(start_date, end_date):
    """Get detailed shift stats including response time and unique users"""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        WITH shift_data AS (
            SELECT
                CASE
                    WHEN EXTRACT(HOUR FROM (session_start AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')) BETWEEN 6 AND 13 THEN 'Morning (6am-2pm)'
                    WHEN EXTRACT(HOUR FROM (session_start AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')) BETWEEN 14 AND 21 THEN 'Mid (2pm-10pm)'
                    ELSE 'Evening (10pm-6am)'
                END as shift,
                conversation_id,
                avg_response_time_seconds
            FROM sessions
            WHERE (session_start AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')::date BETWEEN %s AND %s
        )
        SELECT
            shift,
            COUNT(DISTINCT conversation_id) as unique_convos,
            COUNT(*) as total_sessions,
            AVG(avg_response_time_seconds) FILTER (WHERE avg_response_time_seconds > 0) as avg_response_time
        FROM shift_data
        GROUP BY shift
        ORDER BY
            CASE shift
                WHEN 'Morning (6am-2pm)' THEN 1
                WHEN 'Mid (2pm-10pm)' THEN 2
                ELSE 3
            END
    """, (start_date, end_date))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return pd.DataFrame(rows, columns=['Shift', 'Unique Convos', 'Total Sessions', 'Avg Response Time (s)'])

@st.cache_data(ttl=60)
def get_daily_shift_stats(start_date, end_date):
    """Get daily breakdown by shift with unique users and response time"""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        WITH session_data AS (
            SELECT
                (session_start AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')::date as date,
                CASE
                    WHEN EXTRACT(HOUR FROM (session_start AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')) BETWEEN 6 AND 13 THEN 'Morning'
                    WHEN EXTRACT(HOUR FROM (session_start AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')) BETWEEN 14 AND 21 THEN 'Mid'
                    ELSE 'Evening'
                END as shift,
                conversation_id,
                avg_response_time_seconds
            FROM sessions
            WHERE (session_start AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')::date BETWEEN %s AND %s
        )
        SELECT
            date,
            shift,
            COUNT(DISTINCT conversation_id) as unique_convos,
            AVG(avg_response_time_seconds) FILTER (WHERE avg_response_time_seconds > 0) as avg_response_time
        FROM session_data
        GROUP BY date, shift
        ORDER BY date DESC, shift
    """, (start_date, end_date))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return pd.DataFrame(rows, columns=['Date', 'Shift', 'Unique Convos', 'Avg Response (s)'])

@st.cache_data(ttl=60)
def get_sma_summary(start_date, end_date):
    """Get SMA agent performance summary"""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT
            a.agent_name as "SMA",
            ads.shift as "Shift",
            SUM(ads.messages_received) as "Msg Recv",
            SUM(ads.messages_sent) as "Msg Sent",
            SUM(ads.comment_replies) as "Cmt Reply",
            AVG(ads.avg_response_time_seconds) FILTER (WHERE ads.avg_response_time_seconds > 0) as "Avg RT (s)",
            COUNT(DISTINCT ads.date) FILTER (WHERE ads.schedule_status = 'present') as "Days Present",
            COUNT(DISTINCT ads.date) as "Total Days"
        FROM agents a
        JOIN agent_daily_stats ads ON a.id = ads.agent_id
        WHERE ads.date BETWEEN %s AND %s
          AND a.is_active = true
        GROUP BY a.agent_name, ads.shift
        ORDER BY a.agent_name, ads.shift
    """, (start_date, end_date))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return pd.DataFrame(rows, columns=['SMA', 'Shift', 'Msg Recv', 'Msg Sent', 'Cmt Reply', 'Avg RT (s)', 'Days Present', 'Total Days'])

@st.cache_data(ttl=60)
def get_sma_session_stats(start_date, end_date):
    """Get SMA session stats with unique conversations and human response time (excluding fast bot replies)"""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        WITH agent_pages AS (
            SELECT DISTINCT a.agent_name, apa.page_id, apa.shift
            FROM agents a
            JOIN agent_page_assignments apa ON a.id = apa.agent_id
            WHERE a.is_active = true AND apa.is_active = true
        ),
        session_shifts AS (
            SELECT
                s.*,
                CASE
                    WHEN EXTRACT(HOUR FROM (s.session_start AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')) BETWEEN 6 AND 13 THEN 'Morning'
                    WHEN EXTRACT(HOUR FROM (s.session_start AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')) BETWEEN 14 AND 21 THEN 'Mid'
                    ELSE 'Evening'
                END as computed_shift
            FROM sessions s
            WHERE (s.session_start AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')::date BETWEEN %s AND %s
        )
        SELECT
            ap.agent_name as "SMA",
            ap.shift as "Shift",
            COUNT(DISTINCT ss.conversation_id) as "Unique Users",
            AVG(ss.avg_response_time_seconds) FILTER (WHERE ss.avg_response_time_seconds > 10) as "Avg Human RT (s)"
        FROM agent_pages ap
        LEFT JOIN session_shifts ss ON ap.page_id = ss.page_id AND ap.shift = ss.computed_shift
        GROUP BY ap.agent_name, ap.shift
        ORDER BY ap.agent_name, ap.shift
    """, (start_date, end_date))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return pd.DataFrame(rows, columns=['SMA', 'Shift', 'Unique Users', 'Avg Human RT (s)'])

@st.cache_data(ttl=60)
def get_new_chats_stats(start_date, end_date):
    """Get new chat/conversation count per shift - first message received from users"""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        WITH first_messages AS (
            SELECT
                page_id,
                conversation_id,
                MIN(message_time) as first_msg_time
            FROM messages
            WHERE is_from_page = false
              AND (message_time AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')::date BETWEEN %s AND %s
            GROUP BY page_id, conversation_id
        )
        SELECT
            CASE
                WHEN EXTRACT(HOUR FROM (first_msg_time AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')) BETWEEN 6 AND 13 THEN 'Morning'
                WHEN EXTRACT(HOUR FROM (first_msg_time AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')) BETWEEN 14 AND 21 THEN 'Mid'
                ELSE 'Evening'
            END as shift,
            COUNT(DISTINCT conversation_id) as new_chats
        FROM first_messages
        GROUP BY shift
        ORDER BY
            CASE shift
                WHEN 'Morning' THEN 1
                WHEN 'Mid' THEN 2
                ELSE 3
            END
    """, (start_date, end_date))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return pd.DataFrame(rows, columns=['Shift', 'New Chats'])

@st.cache_data(ttl=60)
def get_sma_new_chats(start_date, end_date):
    """Get new chat count per SMA agent (first messages received)"""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        WITH agent_pages AS (
            SELECT DISTINCT a.agent_name, apa.page_id, apa.shift
            FROM agents a
            JOIN agent_page_assignments apa ON a.id = apa.agent_id
            WHERE a.is_active = true AND apa.is_active = true
        ),
        first_messages AS (
            SELECT
                page_id,
                conversation_id,
                MIN(message_time) as first_msg_time,
                CASE
                    WHEN EXTRACT(HOUR FROM (MIN(message_time) AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')) BETWEEN 6 AND 13 THEN 'Morning'
                    WHEN EXTRACT(HOUR FROM (MIN(message_time) AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')) BETWEEN 14 AND 21 THEN 'Mid'
                    ELSE 'Evening'
                END as computed_shift
            FROM messages
            WHERE is_from_page = false
              AND (message_time AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')::date BETWEEN %s AND %s
            GROUP BY page_id, conversation_id
        )
        SELECT
            ap.agent_name as "SMA",
            ap.shift as "Shift",
            COUNT(DISTINCT fm.conversation_id) as "New Chats"
        FROM agent_pages ap
        LEFT JOIN first_messages fm ON ap.page_id = fm.page_id AND ap.shift = fm.computed_shift
        GROUP BY ap.agent_name, ap.shift
        ORDER BY ap.agent_name, ap.shift
    """, (start_date, end_date))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return pd.DataFrame(rows, columns=['SMA', 'Shift', 'New Chats'])

@st.cache_data(ttl=60)
def get_first_response_stats(start_date, end_date):
    """Get first response time stats per shift (excluding bot responses < 10 seconds)
    Uses sessions table which has populated response time data"""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        WITH session_with_shift AS (
            SELECT
                page_id,
                conversation_id,
                avg_response_time_seconds,
                CASE
                    WHEN EXTRACT(HOUR FROM (session_start AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')) BETWEEN 6 AND 13 THEN 'Morning'
                    WHEN EXTRACT(HOUR FROM (session_start AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')) BETWEEN 14 AND 21 THEN 'Mid'
                    ELSE 'Evening'
                END as shift
            FROM sessions
            WHERE (session_start AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')::date BETWEEN %s AND %s
        )
        SELECT
            shift as "Shift",
            COUNT(DISTINCT conversation_id) as "Unique Convos",
            AVG(avg_response_time_seconds) FILTER (WHERE avg_response_time_seconds > 10) as "Avg First RT (s)"
        FROM session_with_shift
        GROUP BY shift
        ORDER BY
            CASE shift
                WHEN 'Morning' THEN 1
                WHEN 'Mid' THEN 2
                ELSE 3
            END
    """, (start_date, end_date))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return pd.DataFrame(rows, columns=['Shift', 'Unique Convos', 'Avg First RT (s)'])

@st.cache_data(ttl=60)
def get_sma_daily_stats(start_date, end_date, agent_name=None):
    """Get daily stats per SMA agent"""
    conn = get_connection()
    cur = conn.cursor()

    if agent_name and agent_name != "All Agents":
        cur.execute("""
            SELECT
                a.agent_name,
                ads.date,
                ads.shift,
                ads.schedule_status,
                ads.duty_hours,
                ads.messages_received,
                ads.messages_sent,
                ads.comment_replies,
                ads.avg_response_time_seconds
            FROM agents a
            JOIN agent_daily_stats ads ON a.id = ads.agent_id
            WHERE ads.date BETWEEN %s AND %s
              AND a.agent_name = %s
            ORDER BY ads.date DESC, a.agent_name
        """, (start_date, end_date, agent_name))
    else:
        cur.execute("""
            SELECT
                a.agent_name,
                ads.date,
                ads.shift,
                ads.schedule_status,
                ads.duty_hours,
                ads.messages_received,
                ads.messages_sent,
                ads.comment_replies,
                ads.avg_response_time_seconds
            FROM agents a
            JOIN agent_daily_stats ads ON a.id = ads.agent_id
            WHERE ads.date BETWEEN %s AND %s
              AND a.is_active = true
            ORDER BY ads.date DESC, a.agent_name
        """, (start_date, end_date))

    rows = cur.fetchall()
    cur.close()
    conn.close()
    return pd.DataFrame(rows, columns=['SMA', 'Date', 'Shift', 'Status', 'Duty Hours', 'Msg Recv', 'Msg Sent', 'Cmt Reply', 'Avg RT (s)'])

@st.cache_data(ttl=60)
def get_agent_list():
    """Get list of active agents"""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT agent_name FROM agents WHERE is_active = true ORDER BY agent_name")
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return [row[0] for row in rows]

@st.cache_data(ttl=60)
def get_sma_page_assignments():
    """Get SMA agent page assignments"""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT
            a.agent_name,
            apa.shift,
            p.page_name
        FROM agents a
        JOIN agent_page_assignments apa ON a.id = apa.agent_id
        JOIN pages p ON apa.page_id = p.page_id
        WHERE a.is_active = true AND apa.is_active = true
        ORDER BY a.agent_name, apa.shift, p.page_name
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return pd.DataFrame(rows, columns=['SMA', 'Shift', 'Page'])

@st.cache_data(ttl=60)
def get_sma_by_page_breakdown(start_date, end_date):
    """Get detailed breakdown by SMA and Page - messages, comments, response time
    Uses sessions table for response time (messages.response_time_seconds is empty)"""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        WITH agent_pages AS (
            SELECT DISTINCT a.id as agent_id, a.agent_name, apa.page_id, apa.shift, p.page_name
            FROM agents a
            JOIN agent_page_assignments apa ON a.id = apa.agent_id
            JOIN pages p ON apa.page_id = p.page_id
            WHERE a.is_active = true AND apa.is_active = true
        ),
        msg_with_shift AS (
            SELECT
                page_id,
                conversation_id,
                is_from_page,
                CASE
                    WHEN EXTRACT(HOUR FROM (message_time AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')) BETWEEN 6 AND 13 THEN 'Morning'
                    WHEN EXTRACT(HOUR FROM (message_time AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')) BETWEEN 14 AND 21 THEN 'Mid'
                    ELSE 'Evening'
                END as shift
            FROM messages
            WHERE (message_time AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')::date BETWEEN %s AND %s
        ),
        msg_stats AS (
            SELECT
                page_id,
                shift,
                COUNT(*) FILTER (WHERE is_from_page = false) as msg_recv,
                COUNT(*) FILTER (WHERE is_from_page = true) as msg_sent,
                COUNT(DISTINCT conversation_id) FILTER (WHERE is_from_page = false) as new_chats
            FROM msg_with_shift
            GROUP BY page_id, shift
        ),
        session_with_shift AS (
            SELECT
                page_id,
                avg_response_time_seconds,
                CASE
                    WHEN EXTRACT(HOUR FROM (session_start AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')) BETWEEN 6 AND 13 THEN 'Morning'
                    WHEN EXTRACT(HOUR FROM (session_start AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')) BETWEEN 14 AND 21 THEN 'Mid'
                    ELSE 'Evening'
                END as shift
            FROM sessions
            WHERE (session_start AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')::date BETWEEN %s AND %s
        ),
        session_stats AS (
            SELECT
                page_id,
                shift,
                AVG(avg_response_time_seconds) FILTER (WHERE avg_response_time_seconds > 10) as avg_human_rt
            FROM session_with_shift
            GROUP BY page_id, shift
        ),
        cmt_with_shift AS (
            SELECT
                page_id,
                author_id,
                CASE
                    WHEN EXTRACT(HOUR FROM (comment_time AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')) BETWEEN 6 AND 13 THEN 'Morning'
                    WHEN EXTRACT(HOUR FROM (comment_time AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')) BETWEEN 14 AND 21 THEN 'Mid'
                    ELSE 'Evening'
                END as shift
            FROM comments
            WHERE (comment_time AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')::date BETWEEN %s AND %s
        ),
        cmt_stats AS (
            SELECT
                page_id,
                shift,
                COUNT(*) FILTER (WHERE author_id IS NULL OR author_id != page_id) as cmt_recv,
                COUNT(*) FILTER (WHERE author_id IS NOT NULL AND author_id = page_id) as cmt_reply
            FROM cmt_with_shift
            GROUP BY page_id, shift
        )
        SELECT
            ap.agent_name as "SMA",
            ap.page_name as "Page",
            ap.shift as "Shift",
            COALESCE(ms.new_chats, 0) as "New Chats",
            COALESCE(ms.msg_recv, 0) as "Msg Recv",
            COALESCE(ms.msg_sent, 0) as "Msg Sent",
            COALESCE(cs.cmt_reply, 0) as "Cmt Reply",
            ss.avg_human_rt as "Avg Human RT (s)"
        FROM agent_pages ap
        LEFT JOIN msg_stats ms ON ap.page_id = ms.page_id AND ap.shift = ms.shift
        LEFT JOIN session_stats ss ON ap.page_id = ss.page_id AND ap.shift = ss.shift
        LEFT JOIN cmt_stats cs ON ap.page_id = cs.page_id AND ap.shift = cs.shift
        WHERE COALESCE(ms.msg_recv, 0) + COALESCE(ms.msg_sent, 0) + COALESCE(cs.cmt_recv, 0) + COALESCE(cs.cmt_reply, 0) > 0
        ORDER BY ap.agent_name, ap.page_name, ap.shift
    """, (start_date, end_date, start_date, end_date, start_date, end_date))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return pd.DataFrame(rows, columns=['SMA', 'Page', 'Shift', 'New Chats', 'Msg Recv', 'Msg Sent', 'Cmt Reply', 'Avg Human RT (s)'])

# ============================================
# MAIN APP
# ============================================

st.title("👥 SMA Performance Report")

# Get date range from DB
min_date, max_date = get_date_range()

# T+1: Default to yesterday (not today)
today = date.today()
default_end_date = today - timedelta(days=1)  # T+1 = yesterday

# Make sure default_end_date doesn't exceed available data
if max_date and default_end_date > max_date:
    default_end_date = max_date

# Sidebar filters
with st.sidebar:
    st.header("📅 Date Range")
    st.caption("📌 T+1 Report: Defaults to yesterday's data")

    # Quick select
    quick_select = st.selectbox(
        "Quick Select",
        ["Yesterday (T+1)", "Last 7 Days", "Last 14 Days", "Last 30 Days", "Custom"]
    )

    if quick_select == "Yesterday (T+1)":
        end_date = default_end_date
        start_date = default_end_date
    elif quick_select == "Last 7 Days":
        end_date = default_end_date
        start_date = end_date - timedelta(days=6)
    elif quick_select == "Last 14 Days":
        end_date = default_end_date
        start_date = end_date - timedelta(days=13)
    elif quick_select == "Last 30 Days":
        end_date = default_end_date
        start_date = end_date - timedelta(days=29)
    else:
        col1, col2 = st.columns(2)
        with col1:
            start_date = st.date_input("From", value=default_end_date - timedelta(days=6), min_value=min_date, max_value=default_end_date)
        with col2:
            end_date = st.date_input("To", value=default_end_date, min_value=min_date, max_value=default_end_date)

    days_selected = (end_date - start_date).days + 1
    st.caption(f"📆 {days_selected} days selected")
    st.caption(f"From: {start_date}")
    st.caption(f"To: {end_date}")

    st.markdown("---")

    # Page filter for reply verification
    pages_df = get_pages()
    page_options = ["All Pages"] + pages_df['page_name'].tolist()
    selected_page = st.selectbox("🔍 Filter Page (for replies)", page_options)

    st.markdown("---")
    st.subheader("📥 Export Report")

# Get data
totals = get_totals(start_date, end_date)
page_breakdown = get_page_breakdown(start_date, end_date)
comments_by_page = get_comments_by_page(start_date, end_date)
shift_breakdown = get_shift_breakdown(start_date, end_date)

# Header with date range
st.markdown(f"### 📆 {start_date.strftime('%b %d, %Y')} - {end_date.strftime('%b %d, %Y')}")

# Summary metrics - Row 1
col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("📥 Messages Received", f"{totals['msg_recv']:,}")
with col2:
    st.metric("📤 Messages Sent", f"{totals['msg_sent']:,}")
with col3:
    st.metric("💬 Comments Received", f"{totals['cmt_recv']:,}")
with col4:
    st.metric("↩️ Page Replies", f"{totals['cmt_reply']:,}")

# Summary metrics - Row 2
col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("👥 Unique Conversations", f"{totals['unique_convos']:,}")
with col2:
    # Format response time
    avg_rt = totals['avg_response_time']
    if avg_rt:
        if avg_rt >= 3600:
            rt_str = f"{avg_rt/3600:.1f}h"
        elif avg_rt >= 60:
            rt_str = f"{avg_rt/60:.1f}m"
        else:
            rt_str = f"{avg_rt:.0f}s"
    else:
        rt_str = "N/A"
    st.metric("⏱️ Avg Response Time", rt_str)
with col3:
    # Response rate
    if totals['msg_recv'] > 0:
        response_rate = round(100 * totals['msg_sent'] / totals['msg_recv'], 1)
        st.metric("📊 Response Rate", f"{response_rate}%")
    else:
        st.metric("📊 Response Rate", "N/A")
with col4:
    # Comment reply rate
    if totals['cmt_recv'] > 0:
        cmt_rate = round(100 * totals['cmt_reply'] / totals['cmt_recv'], 1)
        st.metric("💬 Comment Reply Rate", f"{cmt_rate}%")
    else:
        st.metric("💬 Comment Reply Rate", "N/A")

st.markdown("---")

# Tabs for different views
tab1, tab2, tab3, tab4, tab5 = st.tabs(["📊 By Page", "🕐 By Shift", "👤 By SMA", "↩️ Reply Verification", "📈 Daily Trend"])

# ============================================
# TAB 1: BY PAGE BREAKDOWN
# ============================================
with tab1:
    st.subheader("📊 Performance by Page")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("**📨 Messages**")
        if not page_breakdown.empty:
            page_breakdown_display = format_dataframe_numbers(page_breakdown, exclude_cols=['Page'])
            st.dataframe(page_breakdown_display, hide_index=True, height=400)
        else:
            st.info("No message data for selected period")

    with col2:
        st.markdown("**💬 Comments**")
        if not comments_by_page.empty:
            comments_display = format_dataframe_numbers(comments_by_page, exclude_cols=['Page'])
            st.dataframe(comments_display, hide_index=True, height=400)
        else:
            st.info("No comment data for selected period")

    # Charts
    st.markdown("---")
    col1, col2 = st.columns(2)

    with col1:
        if not page_breakdown.empty:
            fig = px.bar(
                page_breakdown.head(10),
                x='Page',
                y=['Msg Recv', 'Msg Sent'],
                title='Top 10 Pages - Messages',
                barmode='group'
            )
            fig.update_layout(height=350)
            st.plotly_chart(fig)

    with col2:
        if not comments_by_page.empty:
            fig = px.bar(
                comments_by_page.head(10),
                x='Page',
                y=['Comments Recv', 'Page Replies'],
                title='Top 10 Pages - Comments',
                barmode='group'
            )
            fig.update_layout(height=350)
            st.plotly_chart(fig)

# ============================================
# TAB 2: BY SHIFT BREAKDOWN
# ============================================
with tab2:
    st.subheader("🕐 Performance by Shift")

    # Get shift stats with response time
    shift_stats = get_shift_stats(start_date, end_date)

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("**📨 Message Stats by Shift**")
        if not shift_breakdown.empty:
            shift_display = format_dataframe_numbers(shift_breakdown, exclude_cols=['Shift'])
            st.dataframe(shift_display, hide_index=True)
        else:
            st.info("No message data")

    with col2:
        st.markdown("**⏱️ Response Stats by Shift**")
        if not shift_stats.empty:
            # Format response time and numbers
            display_stats = shift_stats.copy()
            display_stats['Unique Convos'] = display_stats['Unique Convos'].apply(lambda x: f"{int(x):,}" if pd.notna(x) else "N/A")
            display_stats['Total Sessions'] = display_stats['Total Sessions'].apply(lambda x: f"{int(x):,}" if pd.notna(x) else "N/A")
            display_stats['Avg Response Time (s)'] = display_stats['Avg Response Time (s)'].apply(
                lambda x: f"{x/60:.1f}m" if x and x >= 60 else (f"{x:.0f}s" if x else "N/A")
            )
            st.dataframe(display_stats, hide_index=True)
        else:
            st.info("No session data")

    # Charts
    st.markdown("---")
    col1, col2 = st.columns(2)

    with col1:
        if not shift_breakdown.empty:
            fig = go.Figure()
            fig.add_trace(go.Bar(
                name='Received',
                x=shift_breakdown['Shift'],
                y=shift_breakdown['Received'],
                marker_color='#3B82F6'
            ))
            fig.add_trace(go.Bar(
                name='Sent',
                x=shift_breakdown['Shift'],
                y=shift_breakdown['Sent'],
                marker_color='#10B981'
            ))
            fig.update_layout(
                title='Messages by Shift',
                barmode='group',
                height=350
            )
            st.plotly_chart(fig)

    with col2:
        if not shift_stats.empty:
            fig = go.Figure()
            fig.add_trace(go.Bar(
                name='Unique Convos',
                x=shift_stats['Shift'],
                y=shift_stats['Unique Convos'],
                marker_color='#8B5CF6'
            ))
            fig.update_layout(
                title='Unique Conversations by Shift',
                height=350
            )
            st.plotly_chart(fig)

    # Daily breakdown by shift
    st.markdown("---")
    st.markdown("**📅 Daily Breakdown by Shift**")
    daily_shift = get_daily_shift_stats(start_date, end_date)
    if not daily_shift.empty:
        # Format response time and numbers
        daily_shift['Avg Response (s)'] = daily_shift['Avg Response (s)'].apply(
            lambda x: f"{x/60:.1f}m" if x and x >= 60 else (f"{x:.0f}s" if x else "N/A")
        )
        daily_shift['Date'] = pd.to_datetime(daily_shift['Date']).dt.strftime('%Y-%m-%d (%a)')
        # Format numeric columns with commas
        for col in ['Received', 'Sent', 'Response Rate %']:
            if col in daily_shift.columns:
                daily_shift[col] = daily_shift[col].apply(lambda x: f"{int(x):,}" if pd.notna(x) and col != 'Response Rate %' else (f"{x:.1f}" if pd.notna(x) else "N/A"))
        st.dataframe(daily_shift, hide_index=True, height=400)
    else:
        st.info("No daily shift data")

# ============================================
# TAB 3: BY SMA (AGENT)
# ============================================
with tab3:
    st.subheader("👤 Performance by SMA Agent")

    # Get agent list for filter
    agent_list = get_agent_list()

    if not agent_list:
        st.warning("No SMA agent data available. Run the Google Sheets sync first.")
    else:
        # Filter by agent
        col1, col2 = st.columns([1, 3])
        with col1:
            agent_filter = st.selectbox("Filter by Agent", ["All Agents"] + agent_list)

        # Summary table by agent
        st.markdown("---")
        st.markdown("**📊 Agent Performance Summary**")

        sma_summary = get_sma_summary(start_date, end_date)
        sma_session_stats = get_sma_session_stats(start_date, end_date)
        sma_new_chats = get_sma_new_chats(start_date, end_date)

        if not sma_summary.empty:
            # Merge with session stats to get unique users and human response time
            sma_display = sma_summary.copy()

            # Merge new chats
            if not sma_new_chats.empty:
                sma_display = sma_display.merge(
                    sma_new_chats[['SMA', 'Shift', 'New Chats']],
                    on=['SMA', 'Shift'],
                    how='left'
                )

            if not sma_session_stats.empty:
                # Merge on SMA and Shift
                sma_display = sma_display.merge(
                    sma_session_stats[['SMA', 'Shift', 'Unique Users', 'Avg Human RT (s)']],
                    on=['SMA', 'Shift'],
                    how='left'
                )

            # Format numeric columns with commas
            for col in ['New Chats', 'Msg Recv', 'Msg Sent', 'Cmt Reply', 'Days Present', 'Total Days', 'Unique Users']:
                if col in sma_display.columns:
                    sma_display[col] = sma_display[col].apply(lambda x: f"{int(x):,}" if pd.notna(x) else "N/A")

            # Format response times
            sma_display['Avg RT (s)'] = sma_display['Avg RT (s)'].apply(
                lambda x: f"{x/60:.1f}m" if x and x >= 60 else (f"{x:.0f}s" if x else "N/A")
            )

            # Format human response time (excluding bot replies < 10s)
            if 'Avg Human RT (s)' in sma_display.columns:
                sma_display['Avg Human RT (s)'] = sma_display['Avg Human RT (s)'].apply(
                    lambda x: f"{x/60:.1f}m" if x and x >= 60 else (f"{x:.0f}s" if x else "N/A")
                )
                # Rename for clarity
                sma_display = sma_display.rename(columns={'Avg Human RT (s)': 'Human RT (excl bot)'})

            st.dataframe(sma_display, hide_index=True)

            # Totals row
            st.markdown("**Totals:**")
            col1, col2, col3, col4, col5, col6 = st.columns(6)
            with col1:
                if not sma_new_chats.empty and 'New Chats' in sma_new_chats.columns:
                    total_new = sma_new_chats['New Chats'].sum()
                    st.metric("💬 New Chats", f"{int(total_new):,}" if pd.notna(total_new) else "N/A")
                else:
                    st.metric("💬 New Chats", "N/A")
            with col2:
                st.metric("📥 Msg Recv", f"{sma_summary['Msg Recv'].sum():,}")
            with col3:
                st.metric("📤 Msg Sent", f"{sma_summary['Msg Sent'].sum():,}")
            with col4:
                pass  # Cmt Recv metric removed
            with col5:
                st.metric("↩️ Cmt Reply", f"{sma_summary['Cmt Reply'].sum():,}")
            with col6:
                if not sma_session_stats.empty and 'Unique Users' in sma_session_stats.columns:
                    total_unique = sma_session_stats['Unique Users'].sum()
                    st.metric("👥 Unique Users", f"{int(total_unique):,}" if pd.notna(total_unique) else "N/A")
                else:
                    st.metric("👥 Unique Users", "N/A")

            # First Response Stats (excluding bot replies)
            st.markdown("---")
            st.markdown("**⏱️ First Response Time by Shift (Excluding Bot Replies < 10s)**")
            first_response_df = get_first_response_stats(start_date, end_date)

            if not first_response_df.empty:
                fr_display = first_response_df.copy()
                # Format numeric columns with commas
                fr_display['Unique Convos'] = fr_display['Unique Convos'].apply(format_number)
                fr_display['Avg First RT (s)'] = fr_display['Avg First RT (s)'].apply(
                    lambda x: f"{x/60:.1f}m" if x and x >= 60 else (f"{x:.0f}s" if x else "N/A")
                )
                st.dataframe(fr_display, hide_index=True)
            else:
                st.info("No first response data available")

            # Charts
            st.markdown("---")
            col1, col2 = st.columns(2)

            with col1:
                fig = px.bar(
                    sma_summary,
                    x='SMA',
                    y=['Msg Recv', 'Msg Sent'],
                    title='Messages by Agent',
                    barmode='group',
                    color_discrete_map={'Msg Recv': '#3B82F6', 'Msg Sent': '#10B981'}
                )
                fig.update_layout(height=350)
                st.plotly_chart(fig)

            with col2:
                fig = px.bar(
                    sma_summary,
                    x='SMA',
                    y=['Cmt Reply'],
                    title='Comments by Agent',
                    barmode='group',
                    color_discrete_map={'Cmt Reply': '#EC4899'}
                )
                fig.update_layout(height=350)
                st.plotly_chart(fig)
        else:
            st.info("No SMA data for selected period")

        # Daily breakdown by agent
        st.markdown("---")
        st.markdown("**📅 Daily Stats by Agent**")

        sma_daily = get_sma_daily_stats(start_date, end_date, agent_filter if agent_filter != "All Agents" else None)

        if not sma_daily.empty:
            # Format display
            sma_daily_display = sma_daily.copy()
            # Format numeric columns with commas
            for col in ['Msg Recv', 'Msg Sent', 'Cmt Reply']:
                if col in sma_daily_display.columns:
                    sma_daily_display[col] = sma_daily_display[col].apply(format_number)
            sma_daily_display['Avg RT (s)'] = sma_daily_display['Avg RT (s)'].apply(
                lambda x: f"{x/60:.1f}m" if x and x >= 60 else (f"{x:.0f}s" if x else "N/A")
            )
            sma_daily_display['Date'] = pd.to_datetime(sma_daily_display['Date']).dt.strftime('%Y-%m-%d (%a)')

            # Style status column
            def style_status(val):
                if val == 'present':
                    return 'background-color: #dcfce7; color: #166534'
                elif val == 'absent':
                    return 'background-color: #fee2e2; color: #991b1b'
                elif val == 'off':
                    return 'background-color: #fef3c7; color: #92400e'
                return ''

            styled = sma_daily_display.style.map(style_status, subset=['Status'])
            st.dataframe(styled, hide_index=True, height=400)
        else:
            st.info("No daily SMA data for selected period")

        # SMA by Page Breakdown
        st.markdown("---")
        st.markdown("**📄 Distribution by SMA by Page**")

        sma_page_breakdown = get_sma_by_page_breakdown(start_date, end_date)

        if not sma_page_breakdown.empty:
            # Format response time and numeric columns
            sma_page_display = sma_page_breakdown.copy()
            for col in ['New Chats', 'Msg Recv', 'Msg Sent', 'Cmt Reply']:
                if col in sma_page_display.columns:
                    sma_page_display[col] = sma_page_display[col].apply(format_number)
            sma_page_display['Avg Human RT (s)'] = sma_page_display['Avg Human RT (s)'].apply(
                lambda x: f"{x/60:.1f}m" if x and x >= 60 else (f"{x:.0f}s" if x else "N/A")
            )

            # Filter by selected agent
            if agent_filter and agent_filter != "All Agents":
                sma_page_display = sma_page_display[sma_page_display['SMA'] == agent_filter]

            st.dataframe(sma_page_display, hide_index=True, height=400)

            # Summary by page for selected agent
            if agent_filter and agent_filter != "All Agents":
                st.markdown(f"**📊 Page Summary for {agent_filter}:**")
                # Need to use original data for aggregation, then format
                page_data = sma_page_breakdown[sma_page_breakdown['SMA'] == agent_filter]
                page_summary = page_data.groupby('Page').agg({
                    'New Chats': 'sum',
                    'Msg Recv': 'sum',
                    'Msg Sent': 'sum',
                                        'Cmt Reply': 'sum'
                }).reset_index()
                # Format with commas
                for col in ['New Chats', 'Msg Recv', 'Msg Sent', 'Cmt Reply']:
                    page_summary[col] = page_summary[col].apply(format_number)
                st.dataframe(page_summary, hide_index=True)
        else:
            st.info("No SMA by Page data for selected period")

        # Page assignments
        st.markdown("---")
        with st.expander("📋 SMA Page Assignments", expanded=False):
            assignments = get_sma_page_assignments()
            if not assignments.empty:
                st.dataframe(assignments, hide_index=True)
            else:
                st.info("No page assignments found")

# ============================================
# TAB 4: REPLY VERIFICATION
# ============================================
with tab4:
    st.subheader("↩️ Page Reply Verification")
    st.caption("Links to verify page replies on Facebook")

    # Get replies with links
    replies_df = get_page_replies_with_links(start_date, end_date, selected_page)

    if not replies_df.empty:
        st.info(f"Showing {len(replies_df)} most recent page replies" +
                (f" for {selected_page}" if selected_page != "All Pages" else ""))

        # Display with clickable links
        for idx, row in replies_df.iterrows():
            with st.expander(f"🕐 {row['Time (PHT)']} | {row['Page']}", expanded=False):
                st.markdown(f"**Reply:** {row['Reply Text']}...")
                st.markdown(f"🔗 [Open Post on Facebook]({row['Post Link']})")
                st.caption(f"Post ID: {row['Post ID']} | Comment ID: {row['Comment ID']}")

        # Also show as table
        st.markdown("---")
        st.markdown("**📋 Full Table (with links)**")

        st.dataframe(
            replies_df[['Page', 'Time (PHT)', 'Reply Text', 'Post Link']],
            hide_index=True,
            column_config={
                "Post Link": st.column_config.LinkColumn("Post Link", display_text="Open")
            }
        )
    else:
        st.warning("No page replies found for selected period/page")

# ============================================
# TAB 5: DAILY TREND
# ============================================
with tab5:
    st.subheader("📈 Daily Message Trend")

    daily_df = get_daily_summary(start_date, end_date)

    if not daily_df.empty:
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=daily_df['date'],
            y=daily_df['messages_received'],
            name='Received',
            mode='lines+markers',
            line=dict(color='#3B82F6')
        ))
        fig.add_trace(go.Scatter(
            x=daily_df['date'],
            y=daily_df['messages_sent'],
            name='Sent',
            mode='lines+markers',
            line=dict(color='#10B981')
        ))
        fig.update_layout(
            title='Daily Messages (Philippine Time)',
            xaxis_title='Date',
            yaxis_title='Count',
            height=400
        )
        st.plotly_chart(fig)

        # Daily table
        st.markdown("**📋 Daily Breakdown**")
        daily_display = daily_df.copy()
        daily_display['date'] = pd.to_datetime(daily_display['date']).dt.strftime('%Y-%m-%d (%a)')
        daily_display.columns = ['Date', 'Received', 'Sent']
        # Format with commas
        daily_display['Received'] = daily_display['Received'].apply(format_number)
        daily_display['Sent'] = daily_display['Sent'].apply(format_number)
        st.dataframe(daily_display, hide_index=True)
    else:
        st.info("No data for selected period")

# ============================================
# EXPORT FUNCTIONALITY
# ============================================
def generate_sma_html_report():
    """Generate HTML report for PDF export"""
    # Build HTML using string concatenation to avoid f-string multiline issues
    html_parts = []

    html_parts.append('<!DOCTYPE html>')
    html_parts.append('<html>')
    html_parts.append('<head>')
    html_parts.append(f'    <title>SMA Performance Report - {start_date.strftime("%b %d")} to {end_date.strftime("%b %d, %Y")}</title>')
    html_parts.append('    <style>')
    html_parts.append('        body { font-family: Arial, sans-serif; padding: 20px; max-width: 1200px; margin: 0 auto; }')
    html_parts.append('        h1 { color: #1f2937; border-bottom: 2px solid #3B82F6; padding-bottom: 10px; }')
    html_parts.append('        h2 { color: #374151; margin-top: 30px; }')
    html_parts.append('        .header { background: linear-gradient(135deg, #3B82F6, #10B981); color: white; padding: 20px; border-radius: 10px; margin-bottom: 20px; }')
    html_parts.append('        .header h1 { color: white; border: none; margin: 0; }')
    html_parts.append('        .summary-grid { display: grid; grid-template-columns: repeat(4, 1fr); gap: 15px; margin: 20px 0; }')
    html_parts.append('        .metric-card { background: #f3f4f6; padding: 15px; border-radius: 8px; text-align: center; }')
    html_parts.append('        .metric-value { font-size: 24px; font-weight: bold; color: #1f2937; }')
    html_parts.append('        .metric-label { font-size: 12px; color: #6b7280; margin-top: 5px; }')
    html_parts.append('        table { width: 100%; border-collapse: collapse; margin: 15px 0; }')
    html_parts.append('        th, td { border: 1px solid #e5e7eb; padding: 10px; text-align: left; }')
    html_parts.append('        th { background: #f9fafb; font-weight: 600; }')
    html_parts.append('        tr:nth-child(even) { background: #f9fafb; }')
    html_parts.append('        .footer { margin-top: 30px; padding-top: 15px; border-top: 1px solid #e5e7eb; color: #6b7280; font-size: 12px; }')
    html_parts.append('        @media print { body { padding: 10px; } .header { background: #3B82F6 !important; -webkit-print-color-adjust: exact; } }')
    html_parts.append('    </style>')
    html_parts.append('</head>')
    html_parts.append('<body>')
    html_parts.append('    <div class="header">')
    html_parts.append('        <h1>SMA Performance Report</h1>')
    html_parts.append(f'        <p>Period: {start_date.strftime("%b %d, %Y")} - {end_date.strftime("%b %d, %Y")}</p>')
    html_parts.append(f'        <p>Generated: {date.today().strftime("%B %d, %Y")}</p>')
    html_parts.append('    </div>')
    html_parts.append('    <h2>Summary Metrics</h2>')
    html_parts.append('    <div class="summary-grid">')
    html_parts.append('        <div class="metric-card">')
    html_parts.append(f'            <div class="metric-value">{totals["msg_recv"]:,}</div>')
    html_parts.append('            <div class="metric-label">Messages Received</div>')
    html_parts.append('        </div>')
    html_parts.append('        <div class="metric-card">')
    html_parts.append(f'            <div class="metric-value">{totals["msg_sent"]:,}</div>')
    html_parts.append('            <div class="metric-label">Messages Sent</div>')
    html_parts.append('        </div>')
    html_parts.append('        <div class="metric-card">')
    html_parts.append(f'            <div class="metric-value">{totals["cmt_recv"]:,}</div>')
    html_parts.append('            <div class="metric-label">Comments Received</div>')
    html_parts.append('        </div>')
    html_parts.append('        <div class="metric-card">')
    html_parts.append(f'            <div class="metric-value">{totals["cmt_reply"]:,}</div>')
    html_parts.append('            <div class="metric-label">Page Replies</div>')
    html_parts.append('        </div>')
    html_parts.append('    </div>')

    # Add page breakdown table
    if not page_breakdown.empty:
        html_parts.append('    <h2>Performance by Page</h2>')
        html_parts.append('    <table>')
        html_parts.append('        <tr><th>Page</th><th>Msg Recv</th><th>Msg Sent</th><th>Response Rate %</th></tr>')
        for _, row in page_breakdown.iterrows():
            html_parts.append(f'        <tr><td>{row["Page"]}</td><td>{row["Msg Recv"]:,}</td><td>{row["Msg Sent"]:,}</td><td>{row["Response Rate %"]}%</td></tr>')
        html_parts.append('    </table>')

    # Add shift breakdown table
    if not shift_breakdown.empty:
        html_parts.append('    <h2>Performance by Shift</h2>')
        html_parts.append('    <table>')
        html_parts.append('        <tr><th>Shift</th><th>Total</th><th>Received</th><th>Sent</th><th>Response Rate %</th></tr>')
        for _, row in shift_breakdown.iterrows():
            html_parts.append(f'        <tr><td>{row["Shift"]}</td><td>{row["Total Messages"]:,}</td><td>{row["Received"]:,}</td><td>{row["Sent"]:,}</td><td>{row["Response Rate %"]}%</td></tr>')
        html_parts.append('    </table>')

    html_parts.append('    <div class="footer">')
    html_parts.append('        <p><strong>How to save as PDF:</strong> Press Ctrl+P (or Cmd+P on Mac) and select "Save as PDF" as destination</p>')
    html_parts.append('        <p>All times in Philippine Time (UTC+8) | Data from Facebook Graph API</p>')
    html_parts.append('    </div>')
    html_parts.append('</body>')
    html_parts.append('</html>')

    return '\n'.join(html_parts)

# Export buttons in sidebar
with st.sidebar:
    st.markdown("---")
    st.subheader("Export Report")

    filename_base = f"SMA_Report_{start_date.strftime('%Y%m%d')}_to_{end_date.strftime('%Y%m%d')}"

    # HTML Export (for PDF)
    html_report = generate_sma_html_report()
    st.download_button(
        label="Download Report (HTML/PDF)",
        data=html_report,
        file_name=f"{filename_base}.html",
        mime="text/html",
        help="Open in browser, then Print > Save as PDF"
    )
    st.caption("HTML: Open in browser > Print > Save as PDF")

# Footer
st.markdown("---")
st.caption("All times in Philippine Time (UTC+8) | Data from Facebook Graph API")