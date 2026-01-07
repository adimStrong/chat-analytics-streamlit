"""
T+1 Daily Report - Yesterday's Performance Summary
Shows previous day's data for review with date range support
"""

import streamlit as st
import pandas as pd
import psycopg2
from datetime import date, timedelta
import plotly.express as px
import plotly.graph_objects as go
import io

# Page config
st.set_page_config(
    page_title="T+1 Daily Report",
    page_icon="📊",
    layout="wide"
)

# Database connection
DATABASE_URL = "postgresql://postgres:OQKZTvPIBcUUSUowYaFZFNisaAADLwzF@tramway.proxy.rlwy.net:28999/railway"

@st.cache_resource
def get_connection():
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

# Get yesterday's date (T+1 reporting)
today = date.today()
default_date = today - timedelta(days=1)

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
    st.subheader("📥 Export Report")

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
    st.caption(f"Generated: {today.strftime('%B %d, %Y')} | All times in Philippine Time (UTC+8)")

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
        FROM messages
        WHERE is_from_page = false
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
    LEFT JOIN first_messages fm ON m.sender_id = fm.sender_id
    WHERE (m.message_time AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')::date BETWEEN %s AND %s
""", (from_date, to_date, from_date, to_date))
msg_row = cur.fetchone()
msg_recv, msg_sent, unique_users, new_chats = msg_row if msg_row else (0, 0, 0, 0)

# Comments summary (removed Comments Received)
cur.execute("""
    SELECT
        COUNT(*) FILTER (WHERE author_id IS NOT NULL AND author_id = page_id) as replies
    FROM comments
    WHERE (comment_time AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')::date BETWEEN %s AND %s
""", (from_date, to_date))
cmt_row = cur.fetchone()
cmt_reply = cmt_row[0] if cmt_row else 0

# Response rate
response_rate = (msg_sent / msg_recv * 100) if msg_recv > 0 else 0

# Display summary cards
st.subheader("📈 Daily Summary")
col1, col2, col3, col4, col5, col6 = st.columns(6)

with col1:
    st.metric("📥 Messages Received", f"{msg_recv:,}")
with col2:
    st.metric("📤 Messages Sent", f"{msg_sent:,}")
with col3:
    st.metric("👥 Unique Users", f"{unique_users:,}")
with col4:
    st.metric("🆕 New Chats", f"{new_chats:,}")
with col5:
    st.metric("📊 Response Rate", f"{response_rate:.1f}%")
with col6:
    st.metric("↩️ Page Comments", f"{cmt_reply:,}")

st.markdown("---")

# ============================================
# SMA MEMBER PERFORMANCE
# ============================================
st.subheader("👥 SMA Member Performance")

# Helper function to format response time
def format_rt(seconds):
    """Format response time in seconds to human readable format"""
    if pd.isna(seconds) or seconds is None or seconds == 0:
        return "-"
    if seconds < 60:
        return f"{int(seconds)}s"
    elif seconds < 3600:
        mins = int(seconds // 60)
        secs = int(seconds % 60)
        return f"{mins}m {secs}s"
    else:
        hrs = int(seconds // 3600)
        mins = int((seconds % 3600) // 60)
        return f"{hrs}h {mins}m"

# Calculate total days in date range
total_days_in_range = (to_date - from_date).days + 1

cur.execute("""
    WITH agent_pages AS (
        SELECT DISTINCT a.id as agent_id, a.agent_name, apa.page_id, apa.shift
        FROM agents a
        JOIN agent_page_assignments apa ON a.id = apa.agent_id
        WHERE a.is_active = true AND apa.is_active = true
    ),
    -- First message ever per sender (to identify new users)
    first_messages AS (
        SELECT sender_id, MIN(message_time) as first_msg_time
        FROM messages
        WHERE is_from_page = false
        GROUP BY sender_id
    ),
    -- New chats = first-time users (their first message ever is within the date range)
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
    -- Unique users = all distinct users who messaged (including returning)
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
    attendance AS (
        SELECT
            a.agent_name,
            s.shift,
            COUNT(*) FILTER (WHERE s.schedule_status = 'present') as days_present,
            COUNT(*) as total_scheduled_days
        FROM agent_daily_stats s
        JOIN agents a ON s.agent_id = a.id
        WHERE s.date BETWEEN %s AND %s
        GROUP BY a.agent_name, s.shift
    )
    SELECT
        a.agent_name as "Agent",
        s.shift as "Shift",
        s.schedule_status as "Status",
        s.duty_hours as "Hours",
        CASE WHEN s.schedule_status = 'off' THEN 0 ELSE COALESCE(nc.new_chats, 0) END as "New Chats",
        CASE WHEN s.schedule_status = 'off' THEN 0 ELSE COALESCE(uu.unique_users, 0) END as "Unique Users",
        CASE WHEN s.schedule_status = 'off' THEN 0 ELSE SUM(s.messages_received) END as "Msg Recv",
        CASE WHEN s.schedule_status = 'off' THEN 0 ELSE SUM(s.messages_sent) END as "Msg Sent",
        CASE WHEN s.schedule_status = 'off' THEN 0 ELSE SUM(s.comment_replies) END as "Comments",
        CASE WHEN s.schedule_status = 'off' THEN 0
             WHEN SUM(s.messages_received) > 0 THEN ROUND(100.0 * SUM(s.messages_sent) / SUM(s.messages_received), 1)
             ELSE 0 END as "Response %%",
        CASE WHEN s.schedule_status = 'off' THEN NULL ELSE ROUND(AVG(s.avg_response_time_seconds)::numeric, 1) END as "Avg RT",
        CASE WHEN s.schedule_status = 'off' THEN NULL ELSE ROUND(AVG(hrt.human_response_time)::numeric, 1) END as "Human RT",
        COALESCE(att.days_present, 0) as "Days Present",
        COALESCE(att.total_scheduled_days, 0) as "Total Days"
    FROM agent_daily_stats s
    JOIN agents a ON s.agent_id = a.id
    LEFT JOIN new_chats nc ON a.agent_name = nc.agent_name AND s.shift = nc.shift
    LEFT JOIN unique_users uu ON a.agent_name = uu.agent_name AND s.shift = uu.shift
    LEFT JOIN human_rt hrt ON a.agent_name = hrt.agent_name AND s.shift = hrt.shift
    LEFT JOIN attendance att ON a.agent_name = att.agent_name AND s.shift = att.shift
    WHERE s.date BETWEEN %s AND %s
    GROUP BY a.agent_name, s.shift, s.schedule_status, s.duty_hours, nc.new_chats, uu.unique_users, hrt.human_response_time, att.days_present, att.total_scheduled_days
    ORDER BY
        CASE s.shift
            WHEN 'Morning' THEN 1
            WHEN 'Mid' THEN 2
            ELSE 3
        END,
        a.agent_name
""", (from_date, to_date, from_date, to_date, from_date, to_date, from_date, to_date, from_date, to_date, from_date, to_date))
sma_data = cur.fetchall()

if sma_data:
    sma_df = pd.DataFrame(sma_data, columns=['Agent', 'Shift', 'Status', 'Hours', 'New Chats', 'Unique Users', 'Msg Recv', 'Msg Sent', 'Comments', 'Response %', 'Avg RT', 'Human RT', 'Days Present', 'Total Days'])

    # Color code by status
    def style_status(val):
        if val == 'present':
            return 'background-color: #d1fae5; color: #065f46'  # green
        elif val == 'absent':
            return 'background-color: #fee2e2; color: #991b1b'  # red
        elif val == 'off':
            return 'background-color: #f3f4f6; color: #4b5563'  # gray
        return ''

    sma_display = sma_df.copy()
    for col in ['New Chats', 'Unique Users', 'Msg Recv', 'Msg Sent', 'Comments', 'Days Present', 'Total Days']:
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
            st.markdown(f"💬 Comments: **{present_df['Comments'].sum():,}**")
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
        | **Avg RT** | Average Response Time - overall average time to respond to messages (includes automated) |
        | **Human RT** | Human Response Time - average response time from conversation sessions (excludes instant/automated) |
        | **Days Present** | Number of days the agent was marked as "present" in the date range |
        | **Total Days** | Total scheduled days for the agent in the date range |

        **Note:** When status is "off", all metrics show 0 since the agent was not working.
        """)
else:
    st.info("No SMA schedule data for selected date. Schedule may not be synced yet.")

st.markdown("---")

# ============================================
# BY SHIFT BREAKDOWN
# ============================================
st.subheader("🕐 Performance by Shift")

cur.execute("""
    WITH first_messages AS (
        SELECT sender_id, MIN(message_time) as first_msg_time
        FROM messages
        WHERE is_from_page = false
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
        LEFT JOIN first_messages fm ON m.sender_id = fm.sender_id
        WHERE (m.message_time AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')::date BETWEEN %s AND %s
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
""", (from_date, to_date, from_date, to_date))
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
        FROM messages
        WHERE is_from_page = false
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
    GROUP BY p.page_name
    HAVING COUNT(*) FILTER (WHERE m.is_from_page = false) > 0
    ORDER BY received DESC
    LIMIT 10
""", (from_date, to_date, from_date, to_date))
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
        EXTRACT(HOUR FROM (message_time AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila'))::int as hour,
        COUNT(*) FILTER (WHERE is_from_page = false) as received,
        COUNT(*) FILTER (WHERE is_from_page = true) as sent
    FROM messages
    WHERE (message_time AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')::date BETWEEN %s AND %s
    GROUP BY hour
    ORDER BY hour
""", (from_date, to_date))
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
    sma_export = pd.DataFrame(sma_data, columns=['Agent', 'Shift', 'Status', 'Hours', 'New Chats', 'Unique Users', 'Msg Recv', 'Msg Sent', 'Comments', 'Response %', 'Avg RT (s)', 'Human RT (s)', 'Days Present', 'Total Days'])
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
        @media print {{ body {{ padding: 10px; }} .header {{ background: #3B82F6 !important; -webkit-print-color-adjust: exact; }} }}
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
        <tr><th>Agent</th><th>Shift</th><th>Status</th><th>Hours</th><th>New Chats</th><th>Unique Users</th><th>Msg Recv</th><th>Msg Sent</th><th>Comments</th><th>Response %</th><th>Avg RT</th><th>Human RT</th><th>Days</th></tr>
"""
        for row in sma_data:
            status_style = 'background:#d1fae5' if row[2]=='present' else 'background:#fee2e2' if row[2]=='absent' else 'background:#f3f4f6'
            resp_pct = f"{row[9]:.1f}%" if row[9] else "N/A"
            avg_rt = format_rt(row[10]) if row[10] else "-"
            human_rt = format_rt(row[11]) if row[11] else "-"
            days_display = f"{row[12]}/{row[13]}" if row[12] is not None and row[13] is not None else "-"
            html += f"        <tr><td>{row[0]}</td><td>{row[1]}</td><td style='{status_style}'>{row[2]}</td><td>{row[3] or '-'}</td><td>{row[4]:,}</td><td>{row[5]:,}</td><td>{row[6]:,}</td><td>{row[7]:,}</td><td>{row[8]:,}</td><td>{resp_pct}</td><td>{avg_rt}</td><td>{human_rt}</td><td>{days_display}</td></tr>\n"
        html += "    </table>\n"
    
    # Add shift breakdown table
    if shift_data:
        html += """
    <h2>🕐 Performance by Shift</h2>
    <table>
        <tr><th>Shift</th><th>Received</th><th>Sent</th><th>Unique Users</th><th>Response %</th></tr>
"""
        for row in shift_data:
            resp_pct = f"{row[5]:.1f}%" if row[5] else "N/A"
            html += f"        <tr><td>{row[0]}</td><td>{row[1]:,}</td><td>{row[2]:,}</td><td>{row[4]:,}</td><td>{resp_pct}</td></tr>\n"
        html += "    </table>\n"
    
    # Add top pages table
    if page_data:
        html += """
    <h2>📄 Top Pages Performance</h2>
    <table>
        <tr><th>Page</th><th>Received</th><th>Sent</th><th>Unique Users</th></tr>
"""
        for row in page_data:
            html += f"        <tr><td>{row[0]}</td><td>{row[1]:,}</td><td>{row[2]:,}</td><td>{row[4]:,}</td></tr>\n"
        html += "    </table>\n"
    
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

