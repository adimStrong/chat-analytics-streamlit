"""
T+1 Daily Report - Yesterday's Performance Summary
Shows previous day's data for review with date range support
"""

import streamlit as st
import pandas as pd
from datetime import date, timedelta
import plotly.express as px
import plotly.graph_objects as go
import io

# Import shared modules
from config import (
    CORE_PAGES, CORE_PAGES_SQL, TIMEZONE, CACHE_TTL, COLORS,
    LIVESTREAM_PAGES_SQL, SOCMED_PAGES_SQL
)
from db_utils import get_simple_connection as get_connection
from utils import format_number, format_rt, style_status

# Page config
st.set_page_config(
    page_title="T+1 Daily Report",
    page_icon="📊",
    layout="wide"
)

# Get yesterday's date (T+1 reporting)
today = date.today()
default_date = today - timedelta(days=1)

# Get page filter from session state
page_filter_sql = st.session_state.get('page_filter_sql', CORE_PAGES_SQL)
page_filter_name = st.session_state.get('page_filter_name', 'All Pages')

# ============================================
# SIDEBAR - DATE RANGE SELECTION
# ============================================
with st.sidebar:
    st.header("📅 Report Settings")

    # Date range mode toggle
    use_date_range = st.toggle("Use Date Range", value=False)

    if use_date_range:
        col1, col2 = st.columns(2)
        with col1:
            from_date = st.date_input(
                "From Date",
                value=default_date - timedelta(days=6),
                max_value=today - timedelta(days=1),
                key="from_date"
            )
        with col2:
            to_date = st.date_input(
                "To Date",
                value=default_date,
                max_value=today - timedelta(days=1),
                key="to_date"
            )
        # Ensure from_date <= to_date
        if from_date > to_date:
            st.error("From Date must be before To Date")
            from_date, to_date = to_date, from_date
        date_label = f"{from_date.strftime('%b %d')} - {to_date.strftime('%b %d, %Y')}"
    else:
        report_date = st.date_input(
            "Report Date",
            value=default_date,
            max_value=today - timedelta(days=1),
            key="report_date"
        )
        from_date = report_date
        to_date = report_date
        date_label = report_date.strftime('%B %d, %Y (%A)')

    st.markdown("---")

    # Period comparison toggle
    st.subheader("📊 Period Comparison")
    enable_comparison = st.checkbox("Compare with previous period", value=False)

    if enable_comparison:
        # Calculate previous period (same duration)
        period_days = (to_date - from_date).days + 1
        prev_end_date = from_date - timedelta(days=1)
        prev_start_date = prev_end_date - timedelta(days=period_days - 1)
        st.caption(f"Comparing to: {prev_start_date} - {prev_end_date}")
    else:
        prev_start_date = None
        prev_end_date = None

    st.markdown("---")

    # Show filtered pages info
    st.subheader(f"Showing: {page_filter_name}")
    if page_filter_name == "Live Stream":
        page_list = list(LIVESTREAM_PAGES_SQL)
    elif page_filter_name == "Socmed":
        page_list = list(SOCMED_PAGES_SQL)
    else:
        page_list = CORE_PAGES
    for page in page_list:
        st.caption(f"- {page}")

    st.markdown("---")
    st.subheader("📥 Export Report")

# Helper function for calculating change
def calc_change(current, previous):
    if previous is None or previous == 0:
        return None
    return round(((current - previous) / previous) * 100, 1)

# ============================================
# TITLE (After date selection so it updates)
# ============================================
col_logo, col_title = st.columns([0.08, 0.92])
with col_logo:
    st.image("Juan365.jpg", width=60)
with col_title:
    if use_date_range:
        st.markdown(f"## Daily Report &nbsp;&nbsp; | &nbsp;&nbsp; {date_label}")
    else:
        st.markdown(f"## Daily Report &nbsp;&nbsp; | &nbsp;&nbsp; {date_label}")
    st.caption(f"Showing: {page_filter_name} | Generated: {today.strftime('%B %d, %Y')} | All times in Philippine Time (UTC+8)")

st.markdown("---")

# ============================================
# SUMMARY METRICS
# ============================================
conn = get_connection()
cur = conn.cursor()

# Messages summary with unique users and new chats (first-time users)
cur.execute("""
    WITH first_messages AS (
        SELECT sender_id, MIN(message_time) as first_msg_time
        FROM messages m
        JOIN pages p ON m.page_id = p.page_id
        WHERE is_from_page = false AND p.page_name IN %s
        GROUP BY sender_id
    )
    SELECT
        COUNT(*) FILTER (WHERE m.is_from_page = false) as recv,
        COUNT(*) FILTER (WHERE m.is_from_page = true) as sent,
        COUNT(DISTINCT m.sender_id) FILTER (WHERE m.is_from_page = false) as unique_users,
        COUNT(DISTINCT CASE
            WHEN (fm.first_msg_time AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')::date BETWEEN %s AND %s
            THEN m.sender_id
        END) as new_chats
    FROM messages m
    JOIN pages p ON m.page_id = p.page_id
    LEFT JOIN first_messages fm ON m.sender_id = fm.sender_id
    WHERE (m.message_time AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')::date BETWEEN %s AND %s
      AND p.page_name IN %s
""", (page_filter_sql, from_date, to_date, from_date, to_date, page_filter_sql))
msg_row = cur.fetchone()
msg_recv, msg_sent, unique_users, new_chats = msg_row if msg_row else (0, 0, 0, 0)

# Comments summary (removed Comments Received)
cur.execute("""
    SELECT
        COUNT(*) FILTER (WHERE c.author_id IS NOT NULL AND c.author_id = c.page_id) as replies
    FROM comments c
    JOIN pages p ON c.page_id = p.page_id
    WHERE (c.comment_time AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')::date BETWEEN %s AND %s
      AND p.page_name IN %s
""", (from_date, to_date, page_filter_sql))
cmt_row = cur.fetchone()
cmt_reply = cmt_row[0] if cmt_row else 0

# Response rate
response_rate = (msg_sent / msg_recv * 100) if msg_recv > 0 else 0

# Get previous period data if comparison enabled
prev_msg_recv, prev_msg_sent, prev_unique_users, prev_new_chats, prev_cmt_reply = 0, 0, 0, 0, 0
prev_response_rate = 0

if enable_comparison and prev_start_date and prev_end_date:
    # Previous period messages
    cur.execute("""
        WITH first_messages AS (
            SELECT sender_id, MIN(message_time) as first_msg_time
            FROM messages m
            JOIN pages p ON m.page_id = p.page_id
            WHERE is_from_page = false AND p.page_name IN %s
            GROUP BY sender_id
        )
        SELECT
            COUNT(*) FILTER (WHERE m.is_from_page = false) as recv,
            COUNT(*) FILTER (WHERE m.is_from_page = true) as sent,
            COUNT(DISTINCT m.sender_id) FILTER (WHERE m.is_from_page = false) as unique_users,
            COUNT(DISTINCT CASE
                WHEN (fm.first_msg_time AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')::date BETWEEN %s AND %s
                THEN m.sender_id
            END) as new_chats
        FROM messages m
        JOIN pages p ON m.page_id = p.page_id
        LEFT JOIN first_messages fm ON m.sender_id = fm.sender_id
        WHERE (m.message_time AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')::date BETWEEN %s AND %s
          AND p.page_name IN %s
    """, (page_filter_sql, prev_start_date, prev_end_date, prev_start_date, prev_end_date, page_filter_sql))
    prev_msg_row = cur.fetchone()
    prev_msg_recv, prev_msg_sent, prev_unique_users, prev_new_chats = prev_msg_row if prev_msg_row else (0, 0, 0, 0)

    # Previous period comments
    cur.execute("""
        SELECT
            COUNT(*) FILTER (WHERE c.author_id IS NOT NULL AND c.author_id = c.page_id) as replies
        FROM comments c
        JOIN pages p ON c.page_id = p.page_id
        WHERE (c.comment_time AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')::date BETWEEN %s AND %s
          AND p.page_name IN %s
    """, (prev_start_date, prev_end_date, page_filter_sql))
    prev_cmt_row = cur.fetchone()
    prev_cmt_reply = prev_cmt_row[0] if prev_cmt_row else 0

    prev_response_rate = (prev_msg_sent / prev_msg_recv * 100) if prev_msg_recv > 0 else 0

# Display summary cards
st.subheader("📈 Daily Summary")

if enable_comparison and prev_start_date:
    st.caption(f"Compared to {prev_start_date.strftime('%b %d')} - {prev_end_date.strftime('%b %d, %Y')}")

col1, col2, col3, col4, col5, col6 = st.columns(6)

with col1:
    delta = None
    if enable_comparison and prev_msg_recv > 0:
        change = calc_change(msg_recv, prev_msg_recv)
        delta = f"{change:+.1f}%" if change is not None else None
    st.metric("📥 Messages Received", f"{msg_recv:,}", delta)

with col2:
    delta = None
    if enable_comparison and prev_msg_sent > 0:
        change = calc_change(msg_sent, prev_msg_sent)
        delta = f"{change:+.1f}%" if change is not None else None
    st.metric("📤 Messages Sent", f"{msg_sent:,}", delta)

with col3:
    delta = None
    if enable_comparison and prev_unique_users > 0:
        change = calc_change(unique_users, prev_unique_users)
        delta = f"{change:+.1f}%" if change is not None else None
    st.metric("👥 Unique Users", f"{unique_users:,}", delta)

with col4:
    delta = None
    if enable_comparison and prev_new_chats > 0:
        change = calc_change(new_chats, prev_new_chats)
        delta = f"{change:+.1f}%" if change is not None else None
    st.metric("🆕 New Chats", f"{new_chats:,}", delta)

with col5:
    delta = None
    if enable_comparison and prev_response_rate > 0:
        change = calc_change(response_rate, prev_response_rate)
        delta = f"{change:+.1f}%" if change is not None else None
    st.metric("📊 Response Rate", f"{response_rate:.1f}%", delta)

with col6:
    delta = None
    if enable_comparison and prev_cmt_reply > 0:
        change = calc_change(cmt_reply, prev_cmt_reply)
        delta = f"{change:+.1f}%" if change is not None else None
    st.metric("↩️ Page Comments", f"{cmt_reply:,}", delta)

st.markdown("---")

# ============================================
# SMA MEMBER PERFORMANCE
# ============================================
st.subheader("👥 SMA Member Performance")

# format_rt is imported from utils module

# Calculate total days in date range
total_days_in_range = (to_date - from_date).days + 1

# Use aggregated query for date ranges (more than 1 day)
if total_days_in_range > 1:
    # AGGREGATED query for date ranges - combines all days into single row per agent
    cur.execute("""
        WITH agent_pages AS (
            SELECT DISTINCT a.id as agent_id, a.agent_name, apa.page_id, apa.shift
            FROM agents a
            JOIN agent_page_assignments apa ON a.id = apa.agent_id
            WHERE a.is_active = true AND apa.is_active = true
        ),
        first_messages AS (
            SELECT sender_id, MIN(message_time) as first_msg_time
            FROM messages
            WHERE is_from_page = false
            GROUP BY sender_id
        ),
        new_chats AS (
            SELECT
                ap.agent_name,
                ap.shift,
                COUNT(DISTINCT m.sender_id) as new_chats
            FROM agent_pages ap
            JOIN messages m ON ap.page_id = m.page_id
            JOIN first_messages fm ON m.sender_id = fm.sender_id
            WHERE (m.message_time AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')::date BETWEEN %s AND %s
              AND m.is_from_page = false
              AND (fm.first_msg_time AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')::date BETWEEN %s AND %s
              AND CASE ap.shift
                  WHEN 'Morning' THEN EXTRACT(HOUR FROM (m.message_time AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')) BETWEEN 6 AND 13
                  WHEN 'Mid' THEN EXTRACT(HOUR FROM (m.message_time AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')) BETWEEN 14 AND 21
                  ELSE EXTRACT(HOUR FROM (m.message_time AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')) NOT BETWEEN 6 AND 21
              END
            GROUP BY ap.agent_name, ap.shift
        ),
        unique_users AS (
            SELECT
                ap.agent_name,
                ap.shift,
                COUNT(DISTINCT m.sender_id) as unique_users
            FROM agent_pages ap
            JOIN messages m ON ap.page_id = m.page_id
            WHERE (m.message_time AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')::date BETWEEN %s AND %s
              AND m.is_from_page = false
              AND CASE ap.shift
                  WHEN 'Morning' THEN EXTRACT(HOUR FROM (m.message_time AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')) BETWEEN 6 AND 13
                  WHEN 'Mid' THEN EXTRACT(HOUR FROM (m.message_time AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')) BETWEEN 14 AND 21
                  ELSE EXTRACT(HOUR FROM (m.message_time AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')) NOT BETWEEN 6 AND 21
              END
            GROUP BY ap.agent_name, ap.shift
        ),
        human_rt AS (
            SELECT
                ap.agent_name,
                ap.shift,
                AVG(ss.avg_response_time_seconds) as human_response_time
            FROM agent_pages ap
            JOIN sessions ss ON ap.page_id = ss.page_id
            WHERE (ss.session_start AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')::date BETWEEN %s AND %s
              AND ss.avg_response_time_seconds > 0
              AND CASE ap.shift
                  WHEN 'Morning' THEN EXTRACT(HOUR FROM (ss.session_start AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')) BETWEEN 6 AND 13
                  WHEN 'Mid' THEN EXTRACT(HOUR FROM (ss.session_start AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')) BETWEEN 14 AND 21
                  ELSE EXTRACT(HOUR FROM (ss.session_start AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')) NOT BETWEEN 6 AND 21
              END
            GROUP BY ap.agent_name, ap.shift
        ),
        agent_stats_agg AS (
            SELECT
                a.agent_name,
                s.shift,
                SUM(s.messages_received) FILTER (WHERE s.schedule_status = 'present') as messages_received,
                SUM(s.messages_sent) FILTER (WHERE s.schedule_status = 'present') as messages_sent,
                SUM(s.comment_replies) FILTER (WHERE s.schedule_status = 'present') as comment_replies,
                AVG(s.avg_response_time_seconds) FILTER (WHERE s.schedule_status = 'present' AND s.avg_response_time_seconds > 0) as avg_rt,
                SUM(CASE WHEN s.duty_hours ~ '^[0-9.]+$' THEN CAST(s.duty_hours AS NUMERIC) ELSE 0 END) FILTER (WHERE s.schedule_status = 'present') as total_hours,
                COUNT(*) FILTER (WHERE s.schedule_status = 'present') as days_present,
                COUNT(*) as total_days,
                COALESCE(SUM(s.opening_spiels_count) FILTER (WHERE s.schedule_status = 'present'), 0) as opening_spiels,
                COALESCE(SUM(s.closing_spiels_count) FILTER (WHERE s.schedule_status = 'present'), 0) as closing_spiels
            FROM agent_daily_stats s
            JOIN agents a ON s.agent_id = a.id
            WHERE s.date BETWEEN %s AND %s
            GROUP BY a.agent_name, s.shift
        )
        SELECT
            asa.agent_name as "Agent",
            asa.shift as "Shift",
            CASE WHEN asa.days_present > 0 THEN 'present' ELSE 'absent' END as "Status",
            asa.total_hours as "Hours",
            COALESCE(nc.new_chats, 0) as "New Chats",
            COALESCE(uu.unique_users, 0) as "Unique Users",
            COALESCE(asa.messages_received, 0) as "Msg Recv",
            COALESCE(asa.messages_sent, 0) as "Msg Sent",
            COALESCE(asa.comment_replies, 0) as "Comments Sent",
            asa.opening_spiels as "Opening",
            asa.closing_spiels as "Closing",
            CASE WHEN COALESCE(asa.messages_received, 0) > 0
                 THEN ROUND(100.0 * COALESCE(asa.messages_sent, 0) / asa.messages_received, 1)
                 ELSE 0 END as "Response %%",
            ROUND(asa.avg_rt::numeric, 1) as "Avg RT",
            ROUND(hrt.human_response_time::numeric, 1) as "Human RT",
            asa.days_present as "Days Present",
            asa.total_days as "Total Days"
        FROM agent_stats_agg asa
        LEFT JOIN new_chats nc ON asa.agent_name = nc.agent_name AND asa.shift = nc.shift
        LEFT JOIN unique_users uu ON asa.agent_name = uu.agent_name AND asa.shift = uu.shift
        LEFT JOIN human_rt hrt ON asa.agent_name = hrt.agent_name AND asa.shift = hrt.shift
        ORDER BY
            CASE asa.shift
                WHEN 'Morning' THEN 1
                WHEN 'Mid' THEN 2
                ELSE 3
            END,
            asa.agent_name
    """, (from_date, to_date, from_date, to_date, from_date, to_date, from_date, to_date, from_date, to_date))
else:
    # SINGLE DAY query - shows individual status per agent
    cur.execute("""
        WITH agent_pages AS (
            SELECT DISTINCT a.id as agent_id, a.agent_name, apa.page_id, apa.shift
            FROM agents a
            JOIN agent_page_assignments apa ON a.id = apa.agent_id
            WHERE a.is_active = true AND apa.is_active = true
        ),
        first_messages AS (
            SELECT sender_id, MIN(message_time) as first_msg_time
            FROM messages
            WHERE is_from_page = false
            GROUP BY sender_id
        ),
        new_chats AS (
            SELECT
                ap.agent_name,
                ap.shift,
                COUNT(DISTINCT m.sender_id) as new_chats
            FROM agent_pages ap
            JOIN messages m ON ap.page_id = m.page_id
            JOIN first_messages fm ON m.sender_id = fm.sender_id
            WHERE (m.message_time AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')::date BETWEEN %s AND %s
              AND m.is_from_page = false
              AND (fm.first_msg_time AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')::date BETWEEN %s AND %s
              AND CASE ap.shift
                  WHEN 'Morning' THEN EXTRACT(HOUR FROM (m.message_time AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')) BETWEEN 6 AND 13
                  WHEN 'Mid' THEN EXTRACT(HOUR FROM (m.message_time AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')) BETWEEN 14 AND 21
                  ELSE EXTRACT(HOUR FROM (m.message_time AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')) NOT BETWEEN 6 AND 21
              END
            GROUP BY ap.agent_name, ap.shift
        ),
        unique_users AS (
            SELECT
                ap.agent_name,
                ap.shift,
                COUNT(DISTINCT m.sender_id) as unique_users
            FROM agent_pages ap
            JOIN messages m ON ap.page_id = m.page_id
            WHERE (m.message_time AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')::date BETWEEN %s AND %s
              AND m.is_from_page = false
              AND CASE ap.shift
                  WHEN 'Morning' THEN EXTRACT(HOUR FROM (m.message_time AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')) BETWEEN 6 AND 13
                  WHEN 'Mid' THEN EXTRACT(HOUR FROM (m.message_time AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')) BETWEEN 14 AND 21
                  ELSE EXTRACT(HOUR FROM (m.message_time AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')) NOT BETWEEN 6 AND 21
              END
            GROUP BY ap.agent_name, ap.shift
        ),
        human_rt AS (
            SELECT
                ap.agent_name,
                ap.shift,
                AVG(ss.avg_response_time_seconds) as human_response_time
            FROM agent_pages ap
            JOIN sessions ss ON ap.page_id = ss.page_id
            WHERE (ss.session_start AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')::date BETWEEN %s AND %s
              AND ss.avg_response_time_seconds > 0
              AND CASE ap.shift
                  WHEN 'Morning' THEN EXTRACT(HOUR FROM (ss.session_start AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')) BETWEEN 6 AND 13
                  WHEN 'Mid' THEN EXTRACT(HOUR FROM (ss.session_start AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')) BETWEEN 14 AND 21
                  ELSE EXTRACT(HOUR FROM (ss.session_start AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')) NOT BETWEEN 6 AND 21
              END
            GROUP BY ap.agent_name, ap.shift
        )
        SELECT
            a.agent_name as "Agent",
            s.shift as "Shift",
            s.schedule_status as "Status",
            s.duty_hours as "Hours",
            CASE WHEN s.schedule_status != 'present' THEN 0 ELSE COALESCE(nc.new_chats, 0) END as "New Chats",
            CASE WHEN s.schedule_status != 'present' THEN 0 ELSE COALESCE(uu.unique_users, 0) END as "Unique Users",
            CASE WHEN s.schedule_status != 'present' THEN 0 ELSE s.messages_received END as "Msg Recv",
            CASE WHEN s.schedule_status != 'present' THEN 0 ELSE s.messages_sent END as "Msg Sent",
            CASE WHEN s.schedule_status != 'present' THEN 0 ELSE s.comment_replies END as "Comments Sent",
            CASE WHEN s.schedule_status != 'present' THEN 0 ELSE COALESCE(s.opening_spiels_count, 0) END as "Opening",
            CASE WHEN s.schedule_status != 'present' THEN 0 ELSE COALESCE(s.closing_spiels_count, 0) END as "Closing",
            CASE WHEN s.schedule_status != 'present' THEN 0
                 WHEN s.messages_received > 0 THEN ROUND(100.0 * s.messages_sent / s.messages_received, 1)
                 ELSE 0 END as "Response %%",
            CASE WHEN s.schedule_status != 'present' THEN NULL ELSE ROUND(s.avg_response_time_seconds::numeric, 1) END as "Avg RT",
            CASE WHEN s.schedule_status != 'present' THEN NULL ELSE ROUND(hrt.human_response_time::numeric, 1) END as "Human RT",
            CASE WHEN s.schedule_status = 'present' THEN 1 ELSE 0 END as "Days Present",
            1 as "Total Days"
        FROM agent_daily_stats s
        JOIN agents a ON s.agent_id = a.id
        LEFT JOIN new_chats nc ON a.agent_name = nc.agent_name AND s.shift = nc.shift
        LEFT JOIN unique_users uu ON a.agent_name = uu.agent_name AND s.shift = uu.shift
        LEFT JOIN human_rt hrt ON a.agent_name = hrt.agent_name AND s.shift = hrt.shift
        WHERE s.date = %s
        ORDER BY
            CASE s.shift
                WHEN 'Morning' THEN 1
                WHEN 'Mid' THEN 2
                ELSE 3
            END,
            a.agent_name
    """, (from_date, to_date, from_date, to_date, from_date, to_date, from_date, to_date, from_date))
sma_data = cur.fetchall()

if sma_data:
    sma_df = pd.DataFrame(sma_data, columns=['Agent', 'Shift', 'Status', 'Hours', 'New Chats', 'Unique Users', 'Msg Recv', 'Msg Sent', 'Comments Sent', 'Opening', 'Closing', 'Response %', 'Avg RT', 'Human RT', 'Days Present', 'Total Days'])

    # Show aggregation info for date ranges
    if total_days_in_range > 1:
        st.info(f"📊 **Aggregated Data** - Showing totals for {total_days_in_range} days ({from_date.strftime('%b %d')} - {to_date.strftime('%b %d')}). One row per agent with combined metrics.")

    # style_status is imported from utils module
    sma_display = sma_df.copy()
    for col in ['New Chats', 'Unique Users', 'Msg Recv', 'Msg Sent', 'Comments Sent', 'Opening', 'Closing', 'Days Present', 'Total Days']:
        sma_display[col] = sma_display[col].apply(format_number)
    sma_display['Response %'] = sma_display['Response %'].apply(lambda x: f"{x:.1f}%" if pd.notna(x) else "N/A")
    sma_display['Avg RT'] = sma_df['Avg RT'].apply(format_rt)
    sma_display['Human RT'] = sma_df['Human RT'].apply(format_rt)

    col1, col2 = st.columns([3, 1])

    with col1:
        st.dataframe(
            sma_display.style.applymap(style_status, subset=['Status']),
            hide_index=True,
            use_container_width=True
        )

    with col2:
        # Summary by status
        status_counts = sma_df['Status'].value_counts()
        st.markdown("**📊 Attendance Summary**")
        for status, count in status_counts.items():
            emoji = "✅" if status == 'present' else "❌" if status == 'absent' else "⏸️" if status == 'off' else "❓"
            st.markdown(f"{emoji} {status.title()}: **{count}**")

        # Total metrics for present agents
        present_df = sma_df[sma_df['Status'] == 'present']
        if not present_df.empty:
            st.markdown("---")
            st.markdown("**📈 Present Agents Total:**")
            st.markdown(f"🆕 New Chats: **{present_df['New Chats'].sum():,}**")
            st.markdown(f"👥 Unique Users: **{present_df['Unique Users'].sum():,}**")
            st.markdown(f"📥 Msg Recv: **{present_df['Msg Recv'].sum():,}**")
            st.markdown(f"📤 Msg Sent: **{present_df['Msg Sent'].sum():,}**")
            st.markdown(f"💬 Comments Sent: **{present_df['Comments Sent'].sum():,}**")
            st.markdown(f"👋 Opening: **{int(present_df['Opening'].sum()):,}**")
            st.markdown(f"🙏 Closing: **{int(present_df['Closing'].sum()):,}**")
            # Calculate average response times for present agents
            avg_rt_mean = present_df['Avg RT'].dropna().mean()
            human_rt_mean = present_df['Human RT'].dropna().mean()
            st.markdown(f"⏱️ Avg RT: **{format_rt(avg_rt_mean)}**")
            st.markdown(f"👤 Human RT: **{format_rt(human_rt_mean)}**")
            st.markdown(f"📅 Days: **{int(present_df['Days Present'].sum())}/{int(present_df['Total Days'].sum())}**")

    # Explanation of columns
    st.markdown("---")
    with st.expander("ℹ️ Column Definitions"):
        st.markdown("""
        | Column | Description |
        |--------|-------------|
        | **New Chats** | First-time users who started a conversation (never messaged before) |
        | **Unique Users** | All distinct users who messaged (including returning users) |
        | **Hours** | Total duty hours (single day) or sum of hours worked (date range) |
        | **Opening** | Count of agent's opening spiel messages (fuzzy matched, 70% threshold) |
        | **Closing** | Count of agent's closing spiel messages (fuzzy matched, 70% threshold) |
        | **Avg RT** | Average Response Time - overall average time to respond to messages (includes automated) |
        | **Human RT** | Human Response Time - average response time from conversation sessions (excludes instant/automated) |
        | **Days Present** | Number of days the agent was marked as "present" in the date range |
        | **Total Days** | Total scheduled days for the agent in the date range |

        **Single Day Mode:** Shows individual agent status (present/absent/off) for that specific day.

        **Date Range Mode:** Aggregates all data into one row per agent. Status shows "present" if agent worked at least 1 day, metrics are summed/averaged across all present days.

        **Spiel Tracking:** Opening/Closing counts track usage of each agent's unique spiels (started Jan 19, 2026).
        """)
else:
    st.info("No SMA schedule data for selected date. Schedule may not be synced yet.")

st.markdown("---")

# ============================================
# SMA PERFORMANCE BY PAGE (Matrix Table)
# ============================================
st.subheader("📄 SMA Performance by Page")

cur.execute("""
    WITH agent_pages AS (
        SELECT DISTINCT a.id as agent_id, a.agent_name, apa.page_id, apa.shift, p.page_name
        FROM agents a
        JOIN agent_page_assignments apa ON a.id = apa.agent_id
        JOIN pages p ON apa.page_id = p.page_id
        WHERE a.is_active = true AND apa.is_active = true
    )
    SELECT
        ap.agent_name,
        ap.page_name,
        ap.shift,
        COUNT(*) FILTER (WHERE m.is_from_page = false) as msg_recv,
        COUNT(*) FILTER (WHERE m.is_from_page = true) as msg_sent,
        COUNT(DISTINCT m.sender_id) FILTER (WHERE m.is_from_page = false) as unique_users
    FROM agent_pages ap
    LEFT JOIN messages m ON ap.page_id = m.page_id
        AND (m.message_time AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')::date BETWEEN %s AND %s
        AND CASE ap.shift
            WHEN 'Morning' THEN EXTRACT(HOUR FROM (m.message_time AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')) BETWEEN 6 AND 13
            WHEN 'Mid' THEN EXTRACT(HOUR FROM (m.message_time AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')) BETWEEN 14 AND 21
            ELSE EXTRACT(HOUR FROM (m.message_time AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')) NOT BETWEEN 6 AND 21
        END
    GROUP BY ap.agent_name, ap.page_name, ap.shift
    HAVING COUNT(*) FILTER (WHERE m.is_from_page = false) > 0 OR COUNT(*) FILTER (WHERE m.is_from_page = true) > 0
    ORDER BY ap.agent_name, ap.shift, msg_recv DESC
""", (from_date, to_date))
page_matrix_data = cur.fetchall()

if page_matrix_data:
    page_matrix_df = pd.DataFrame(page_matrix_data, columns=['Agent', 'Page', 'Shift', 'Msg Recv', 'Msg Sent', 'Unique Users'])

    # Format numbers
    page_matrix_display = page_matrix_df.copy()
    for col in ['Msg Recv', 'Msg Sent', 'Unique Users']:
        page_matrix_display[col] = page_matrix_display[col].apply(format_number)

    st.dataframe(page_matrix_display, hide_index=True, use_container_width=True)

    with st.expander("ℹ️ About this table"):
        st.markdown("""
        This table shows each agent's performance broken down by the pages they manage.
        - **Page**: The Facebook page the agent is assigned to
        - **Shift**: The shift during which the agent manages this page
        - **Metrics**: Messages and users during the agent's shift hours on each page
        """)
else:
    st.info("No page-level data available for the selected date range.")

st.markdown("---")

# ============================================
# BY SHIFT BREAKDOWN
# ============================================
st.subheader("🕐 Performance by Shift")

cur.execute("""
    WITH first_messages AS (
        SELECT sender_id, MIN(message_time) as first_msg_time
        FROM messages m
        JOIN pages p ON m.page_id = p.page_id
        WHERE is_from_page = false AND p.page_name IN %s
        GROUP BY sender_id
    ),
    msg_shift AS (
        SELECT
            m.is_from_page,
            m.sender_id,
            fm.first_msg_time,
            CASE
                WHEN EXTRACT(HOUR FROM (m.message_time AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')) BETWEEN 6 AND 13 THEN 'Morning (6am-2pm)'
                WHEN EXTRACT(HOUR FROM (m.message_time AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')) BETWEEN 14 AND 21 THEN 'Mid (2pm-10pm)'
                ELSE 'Evening (10pm-6am)'
            END as shift
        FROM messages m
        JOIN pages p ON m.page_id = p.page_id
        LEFT JOIN first_messages fm ON m.sender_id = fm.sender_id
        WHERE (m.message_time AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')::date BETWEEN %s AND %s
          AND p.page_name IN %s
    )
    SELECT
        shift,
        COUNT(*) FILTER (WHERE is_from_page = false) as received,
        COUNT(*) FILTER (WHERE is_from_page = true) as sent,
        COUNT(DISTINCT CASE
            WHEN (first_msg_time AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')::date BETWEEN %s AND %s
            THEN sender_id
        END) as new_chats,
        COUNT(DISTINCT sender_id) FILTER (WHERE is_from_page = false) as unique_users,
        ROUND(100.0 * COUNT(*) FILTER (WHERE is_from_page = true) / NULLIF(COUNT(*) FILTER (WHERE is_from_page = false), 0), 1) as response_rate
    FROM msg_shift
    GROUP BY shift
    ORDER BY CASE shift
        WHEN 'Morning (6am-2pm)' THEN 1
        WHEN 'Mid (2pm-10pm)' THEN 2
        ELSE 3
    END
""", (page_filter_sql, from_date, to_date, page_filter_sql, from_date, to_date))
shift_data = cur.fetchall()

if shift_data:
    shift_df = pd.DataFrame(shift_data, columns=['Shift', 'Received', 'Sent', 'New Chats', 'Unique Users', 'Response %'])

    col1, col2 = st.columns([1, 1])

    with col1:
        shift_display = shift_df.copy()
        for col in ['Received', 'Sent', 'New Chats', 'Unique Users']:
            shift_display[col] = shift_display[col].apply(format_number)
        shift_display['Response %'] = shift_display['Response %'].apply(lambda x: f"{x:.1f}%" if pd.notna(x) else "N/A")
        st.dataframe(shift_display, hide_index=True)

    with col2:
        fig_shift = px.bar(
            shift_df,
            x='Shift',
            y=['Received', 'Sent'],
            barmode='group',
            title='Messages by Shift',
            color_discrete_map={'Received': '#3B82F6', 'Sent': '#10B981'}
        )
        fig_shift.update_layout(height=300)
        st.plotly_chart(fig_shift)
else:
    st.info("No shift data available")

st.markdown("---")

# ============================================
# TOP PAGES
# ============================================
st.subheader("📄 Top Pages Performance")

cur.execute("""
    WITH first_messages AS (
        SELECT sender_id, MIN(message_time) as first_msg_time
        FROM messages m
        JOIN pages p ON m.page_id = p.page_id
        WHERE is_from_page = false AND p.page_name IN %s
        GROUP BY sender_id
    )
    SELECT
        p.page_name,
        COUNT(*) FILTER (WHERE m.is_from_page = false) as received,
        COUNT(*) FILTER (WHERE m.is_from_page = true) as sent,
        COUNT(DISTINCT CASE
            WHEN (fm.first_msg_time AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')::date BETWEEN %s AND %s
            THEN m.sender_id
        END) as new_chats,
        COUNT(DISTINCT m.sender_id) FILTER (WHERE m.is_from_page = false) as unique_users
    FROM messages m
    JOIN pages p ON m.page_id = p.page_id
    LEFT JOIN first_messages fm ON m.sender_id = fm.sender_id
    WHERE (m.message_time AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')::date BETWEEN %s AND %s
      AND p.page_name IN %s
    GROUP BY p.page_name
    HAVING COUNT(*) FILTER (WHERE m.is_from_page = false) > 0
    ORDER BY received DESC
    LIMIT 10
""", (page_filter_sql, from_date, to_date, from_date, to_date, page_filter_sql))
page_data = cur.fetchall()

if page_data:
    page_df = pd.DataFrame(page_data, columns=['Page', 'Received', 'Sent', 'New Chats', 'Unique Users'])

    col1, col2 = st.columns([1, 1])

    with col1:
        page_display = page_df.copy()
        for col in ['Received', 'Sent', 'New Chats', 'Unique Users']:
            page_display[col] = page_display[col].apply(format_number)
        st.dataframe(page_display, hide_index=True)

    with col2:
        fig_pages = px.bar(
            page_df.head(10),
            x='Page',
            y='Received',
            title='Top 10 Pages by Messages Received',
            color_discrete_sequence=['#3B82F6']
        )
        fig_pages.update_layout(height=350, xaxis_tickangle=-45)
        st.plotly_chart(fig_pages)
else:
    st.info("No page data available")

st.markdown("---")

# ============================================
# HOURLY TREND
# ============================================
st.subheader("⏰ Hourly Message Trend")

cur.execute("""
    SELECT
        EXTRACT(HOUR FROM (m.message_time AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila'))::int as hour,
        COUNT(*) FILTER (WHERE m.is_from_page = false) as received,
        COUNT(*) FILTER (WHERE m.is_from_page = true) as sent
    FROM messages m
    JOIN pages p ON m.page_id = p.page_id
    WHERE (m.message_time AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')::date BETWEEN %s AND %s
      AND p.page_name IN %s
    GROUP BY hour
    ORDER BY hour
""", (from_date, to_date, page_filter_sql))
hourly_data = cur.fetchall()

if hourly_data:
    hourly_df = pd.DataFrame(hourly_data, columns=['Hour', 'Received', 'Sent'])

    fig_hourly = go.Figure()
    fig_hourly.add_trace(go.Scatter(
        x=hourly_df['Hour'],
        y=hourly_df['Received'],
        name='Received',
        mode='lines+markers',
        line=dict(color='#3B82F6')
    ))
    fig_hourly.add_trace(go.Scatter(
        x=hourly_df['Hour'],
        y=hourly_df['Sent'],
        name='Sent',
        mode='lines+markers',
        line=dict(color='#10B981')
    ))
    fig_hourly.update_layout(
        title=f'Hourly Message Volume - {date_label}',
        xaxis_title='Hour (PHT)',
        yaxis_title='Messages',
        xaxis=dict(tickmode='linear', tick0=0, dtick=2),
        height=350
    )
    st.plotly_chart(fig_hourly)
else:
    st.info("No hourly data available")

# ============================================
# EXPORT FUNCTIONALITY (in sidebar)
# ============================================
export_data = {
    'Metric': ['Messages Received', 'Messages Sent', 'Unique Users', 'New Chats', 'Response Rate', 'Page Comments'],
    'Value': [msg_recv, msg_sent, unique_users, new_chats, f"{response_rate:.1f}%", cmt_reply]
}
export_df = pd.DataFrame(export_data)

csv_buffer = io.StringIO()
csv_buffer.write(f"T+1 Daily Report - {date_label}\n")
csv_buffer.write(f"Generated on: {today.strftime('%B %d, %Y')}\n\n")
csv_buffer.write("SUMMARY\n")
export_df.to_csv(csv_buffer, index=False)
csv_buffer.write("\n")

if sma_data:
    csv_buffer.write("SMA MEMBER PERFORMANCE\n")
    sma_export = pd.DataFrame(sma_data, columns=['Agent', 'Shift', 'Status', 'Hours', 'New Chats', 'Unique Users', 'Msg Recv', 'Msg Sent', 'Comments Sent', 'Opening', 'Closing', 'Response %', 'Avg RT (s)', 'Human RT (s)', 'Days Present', 'Total Days'])
    sma_export.to_csv(csv_buffer, index=False)
    csv_buffer.write("\n")

if shift_data:
    csv_buffer.write("BY SHIFT\n")
    shift_df.to_csv(csv_buffer, index=False)
    csv_buffer.write("\n")

if page_data:
    csv_buffer.write("TOP PAGES\n")
    page_df.to_csv(csv_buffer, index=False)
    csv_buffer.write("\n")

if hourly_data:
    csv_buffer.write("HOURLY TREND\n")
    hourly_df.to_csv(csv_buffer, index=False)

csv_data = csv_buffer.getvalue()

# Generate HTML report for PDF export
def generate_html_report():
    html = f"""<!DOCTYPE html>
<html>
<head>
    <title>Juan365 Daily Report - {date_label}</title>
    <style>
        body {{ font-family: Arial, sans-serif; padding: 20px; max-width: 1100px; margin: 0 auto; }}
        h1 {{ color: #1f2937; border-bottom: 2px solid #3B82F6; padding-bottom: 10px; }}
        h2 {{ color: #374151; margin-top: 30px; }}
        .header {{ background: linear-gradient(135deg, #3B82F6, #10B981); color: white; padding: 20px; border-radius: 10px; margin-bottom: 20px; display: flex; align-items: center; gap: 20px; }}
        .header img {{ width: 60px; height: 60px; border-radius: 8px; }}
        .header h1 {{ color: white; border: none; margin: 0; }}
        .header-text {{ flex: 1; }}
        .summary-grid {{ display: grid; grid-template-columns: repeat(6, 1fr); gap: 15px; margin: 20px 0; }}
        .metric-card {{ background: #f3f4f6; padding: 15px; border-radius: 8px; text-align: center; }}
        .metric-value {{ font-size: 24px; font-weight: bold; color: #1f2937; }}
        .metric-label {{ font-size: 12px; color: #6b7280; margin-top: 5px; }}
        table {{ width: 100%; border-collapse: collapse; margin: 15px 0; font-size: 13px; }}
        th, td {{ border: 1px solid #e5e7eb; padding: 8px; text-align: left; }}
        th {{ background: #f9fafb; font-weight: 600; }}
        tr:nth-child(even) {{ background: #f9fafb; }}
        .footer {{ margin-top: 30px; padding-top: 15px; border-top: 1px solid #e5e7eb; color: #6b7280; font-size: 12px; }}
        /* Chart styles */
        .chart-container {{ margin: 20px 0; }}
        .bar-chart {{ display: flex; flex-direction: column; gap: 8px; }}
        .bar-row {{ display: flex; align-items: center; gap: 10px; }}
        .bar-label {{ width: 120px; font-size: 12px; text-align: right; }}
        .bar-wrapper {{ flex: 1; display: flex; gap: 4px; align-items: center; }}
        .bar {{ height: 24px; border-radius: 4px; display: flex; align-items: center; justify-content: flex-end; padding-right: 6px; color: white; font-size: 11px; font-weight: bold; min-width: 30px; }}
        .bar-recv {{ background: #3B82F6; }}
        .bar-sent {{ background: #10B981; }}
        .chart-legend {{ display: flex; gap: 20px; margin-top: 10px; font-size: 12px; }}
        .legend-item {{ display: flex; align-items: center; gap: 5px; }}
        .legend-color {{ width: 16px; height: 16px; border-radius: 3px; }}
        .hourly-chart {{ display: flex; align-items: flex-end; gap: 2px; height: 120px; border-bottom: 1px solid #e5e7eb; padding-bottom: 5px; }}
        .hourly-bar {{ display: flex; flex-direction: column; align-items: center; gap: 2px; }}
        .hourly-bar-inner {{ width: 18px; border-radius: 2px 2px 0 0; }}
        .hourly-label {{ font-size: 9px; color: #6b7280; }}
        @media print {{ body {{ padding: 10px; }} .header {{ background: #3B82F6 !important; -webkit-print-color-adjust: exact; }} .bar, .legend-color, .hourly-bar-inner {{ -webkit-print-color-adjust: exact; print-color-adjust: exact; }} }}
    </style>
</head>
<body>
    <div class="header">
        <div class="header-text">
            <h1>📊 Juan365 Daily Report</h1>
            <p>Report Date: {date_label}</p>
            <p>Generated: {today.strftime('%B %d, %Y')}</p>
        </div>
    </div>
    
    <h2>📈 Daily Summary</h2>
    <div class="summary-grid">
        <div class="metric-card">
            <div class="metric-value">{msg_recv:,}</div>
            <div class="metric-label">📥 Messages Received</div>
        </div>
        <div class="metric-card">
            <div class="metric-value">{msg_sent:,}</div>
            <div class="metric-label">📤 Messages Sent</div>
        </div>
        <div class="metric-card">
            <div class="metric-value">{unique_users:,}</div>
            <div class="metric-label">👥 Unique Users</div>
        </div>
        <div class="metric-card">
            <div class="metric-value">{new_chats:,}</div>
            <div class="metric-label">🆕 New Chats</div>
        </div>
        <div class="metric-card">
            <div class="metric-value">{response_rate:.1f}%</div>
            <div class="metric-label">📊 Response Rate</div>
        </div>
        <div class="metric-card">
            <div class="metric-value">{cmt_reply:,}</div>
            <div class="metric-label">↩️ Page Comments</div>
        </div>
    </div>
"""
    
    # Add SMA Performance table
    if sma_data:
        html += """
    <h2>👥 SMA Member Performance</h2>
    <table>
        <tr><th>Agent</th><th>Shift</th><th>Status</th><th>Hours</th><th>New Chats</th><th>Unique Users</th><th>Msg Recv</th><th>Msg Sent</th><th>Comments</th><th>Opening</th><th>Closing</th><th>Resp %</th><th>Avg RT</th><th>Human RT</th><th>Days</th></tr>
"""
        for row in sma_data:
            status_style = 'background:#d1fae5' if row[2]=='present' else 'background:#fee2e2' if row[2]=='absent' else 'background:#f3f4f6'
            resp_pct = f"{row[11]:.1f}%" if row[11] else "N/A"
            avg_rt = format_rt(row[12]) if row[12] else "-"
            human_rt = format_rt(row[13]) if row[13] else "-"
            days_display = f"{row[14]}/{row[15]}" if row[14] is not None and row[15] is not None else "-"
            opening = int(row[9]) if row[9] else 0
            closing = int(row[10]) if row[10] else 0
            html += f"        <tr><td>{row[0]}</td><td>{row[1]}</td><td style='{status_style}'>{row[2]}</td><td>{row[3] or '-'}</td><td>{row[4]:,}</td><td>{row[5]:,}</td><td>{row[6]:,}</td><td>{row[7]:,}</td><td>{row[8]:,}</td><td>{opening}</td><td>{closing}</td><td>{resp_pct}</td><td>{avg_rt}</td><td>{human_rt}</td><td>{days_display}</td></tr>\n"
        html += "    </table>\n"

    # Add SMA Performance by Page table
    if page_matrix_data:
        html += """
    <h2>📄 SMA Performance by Page</h2>
    <table>
        <tr><th>Agent</th><th>Page</th><th>Shift</th><th>Msg Recv</th><th>Msg Sent</th><th>Unique Users</th></tr>
"""
        for row in page_matrix_data:
            html += f"        <tr><td>{row[0]}</td><td>{row[1]}</td><td>{row[2]}</td><td>{row[3]:,}</td><td>{row[4]:,}</td><td>{row[5]:,}</td></tr>\n"
        html += "    </table>\n"

    # Add shift breakdown table with chart
    if shift_data:
        html += """
    <h2>🕐 Performance by Shift</h2>
    <table>
        <tr><th>Shift</th><th>Received</th><th>Sent</th><th>New Chats</th><th>Unique Users</th><th>Response %</th></tr>
"""
        for row in shift_data:
            resp_pct = f"{row[5]:.1f}%" if row[5] else "N/A"
            html += f"        <tr><td>{row[0]}</td><td>{row[1]:,}</td><td>{row[2]:,}</td><td>{row[3]:,}</td><td>{row[4]:,}</td><td>{resp_pct}</td></tr>\n"
        html += "    </table>\n"

        # Add shift bar chart
        max_shift_val = max(row[1] for row in shift_data) if shift_data else 1
        html += '    <div class="chart-container"><div class="bar-chart">\n'
        for row in shift_data:
            recv_width = int((row[1] / max_shift_val) * 200) if max_shift_val > 0 else 0
            sent_width = int((row[2] / max_shift_val) * 200) if max_shift_val > 0 else 0
            html += f'        <div class="bar-row"><div class="bar-label">{row[0][:15]}</div><div class="bar-wrapper"><div class="bar bar-recv" style="width:{recv_width}px">{row[1]:,}</div><div class="bar bar-sent" style="width:{sent_width}px">{row[2]:,}</div></div></div>\n'
        html += '    </div>\n'
        html += '    <div class="chart-legend"><div class="legend-item"><div class="legend-color" style="background:#3B82F6"></div>Received</div><div class="legend-item"><div class="legend-color" style="background:#10B981"></div>Sent</div></div></div>\n'

    # Add top pages table with chart
    if page_data:
        html += """
    <h2>📄 Top Pages Performance</h2>
    <table>
        <tr><th>Page</th><th>Received</th><th>Sent</th><th>New Chats</th><th>Unique Users</th></tr>
"""
        for row in page_data:
            html += f"        <tr><td>{row[0]}</td><td>{row[1]:,}</td><td>{row[2]:,}</td><td>{row[3]:,}</td><td>{row[4]:,}</td></tr>\n"
        html += "    </table>\n"

        # Add top pages bar chart (top 5)
        top_pages = page_data[:5]
        max_page_val = max(row[1] for row in top_pages) if top_pages else 1
        html += '    <div class="chart-container"><div class="bar-chart">\n'
        for row in top_pages:
            recv_width = int((row[1] / max_page_val) * 250) if max_page_val > 0 else 0
            html += f'        <div class="bar-row"><div class="bar-label">{row[0][:15]}</div><div class="bar-wrapper"><div class="bar bar-recv" style="width:{recv_width}px">{row[1]:,}</div></div></div>\n'
        html += '    </div></div>\n'

    # Add hourly trend chart
    if hourly_data:
        html += '    <h2>⏰ Hourly Message Trend</h2>\n'
        max_hourly = max(row[1] for row in hourly_data) if hourly_data else 1
        html += '    <div class="chart-container"><div class="hourly-chart">\n'
        for row in hourly_data:
            recv_height = int((row[1] / max_hourly) * 100) if max_hourly > 0 else 0
            sent_height = int((row[2] / max_hourly) * 100) if max_hourly > 0 else 0
            html += f'        <div class="hourly-bar"><div class="hourly-bar-inner bar-recv" style="height:{recv_height}px"></div><div class="hourly-bar-inner bar-sent" style="height:{sent_height}px"></div><div class="hourly-label">{row[0]:02d}</div></div>\n'
        html += '    </div>\n'
        html += '    <div class="chart-legend"><div class="legend-item"><div class="legend-color" style="background:#3B82F6"></div>Received</div><div class="legend-item"><div class="legend-color" style="background:#10B981"></div>Sent</div></div></div>\n'

    html += """
    <div class="footer">
        <p><strong>How to save as PDF:</strong> Press Ctrl+P (or Cmd+P on Mac) → Select "Save as PDF" as destination</p>
        <p>All times in Philippine Time (UTC+8) | Data from Facebook Graph API</p>
    </div>
</body>
</html>"""
    return html

with st.sidebar:
    filename_base = f"T1_Report_{from_date.strftime('%Y%m%d')}"
    if use_date_range:
        filename_base += f"_to_{to_date.strftime('%Y%m%d')}"

    # HTML Export (for PDF)
    html_report = generate_html_report()
    st.download_button(
        label="📄 Download Report (HTML/PDF)",
        data=html_report,
        file_name=f"{filename_base}.html",
        mime="text/html",
        help="Open in browser, then Print > Save as PDF"
    )
    
    # CSV Export
    st.download_button(
        label="📥 Download Data (CSV)",
        data=csv_data,
        file_name=f"{filename_base}.csv",
        mime="text/csv"
    )
    st.caption("HTML: Open in browser → Print → Save as PDF")

cur.close()

# Footer
st.markdown("---")
st.caption("""
**Metric Definitions:**
- **Messages Received**: Incoming messages from users
- **Messages Sent**: Outgoing replies from page
- **Unique Users**: Distinct users who sent messages (including returning)
- **New Chats**: First-time users (never messaged before)
- **Page Comments**: Comments posted by the page (replies to users)
- **Response Rate**: Messages Sent / Messages Received x 100%
""")
st.caption("All times in Philippine Time (UTC+8) | Data from Facebook Graph API")

