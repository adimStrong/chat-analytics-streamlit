"""
T+1 Daily Report - Yesterday's Performance Summary
Shows previous day's data for review
"""

import streamlit as st
import pandas as pd
import psycopg2
from datetime import date, timedelta
import plotly.express as px
import plotly.graph_objects as go

# Page config
st.set_page_config(
    page_title="T+1 Daily Report",
    page_icon="📊",
    layout="wide"
)

# Database connection
DATABASE_URL = "postgresql://postgres:OQKZTvPIBcUUSUowYaFZFNisaAADLwzF@tramway.proxy.rlwy.net:28999/railway"

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
report_date = today - timedelta(days=1)

st.title("📊 T+1 Daily Report")
st.markdown(f"### Report Date: **{report_date.strftime('%B %d, %Y (%A)')}**")
st.caption(f"Generated on: {today.strftime('%B %d, %Y')} | All times in Philippine Time (UTC+8)")

# Allow date override
with st.sidebar:
    st.header("📅 Report Settings")
    selected_date = st.date_input(
        "Report Date",
        value=report_date,
        max_value=today - timedelta(days=1)
    )
    report_date = selected_date

st.markdown("---")

# ============================================
# SUMMARY METRICS
# ============================================
conn = get_connection()
cur = conn.cursor()

# Messages summary
cur.execute("""
    SELECT
        COUNT(*) FILTER (WHERE is_from_page = false) as recv,
        COUNT(*) FILTER (WHERE is_from_page = true) as sent,
        COUNT(DISTINCT conversation_id) FILTER (WHERE is_from_page = false) as new_convos
    FROM messages
    WHERE (message_time AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')::date = %s
""", (report_date,))
msg_row = cur.fetchone()
msg_recv, msg_sent, new_convos = msg_row if msg_row else (0, 0, 0)

# Comments summary
cur.execute("""
    SELECT
        COUNT(*) FILTER (WHERE author_id IS NULL OR author_id != page_id) as recv,
        COUNT(*) FILTER (WHERE author_id IS NOT NULL AND author_id = page_id) as replies
    FROM comments
    WHERE (comment_time AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')::date = %s
""", (report_date,))
cmt_row = cur.fetchone()
cmt_recv, cmt_reply = cmt_row if cmt_row else (0, 0)

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
    st.metric("💬 New Conversations", f"{new_convos:,}")
with col4:
    st.metric("📊 Response Rate", f"{response_rate:.1f}%")
with col5:
    st.metric("💬 Comments Received", f"{cmt_recv:,}")
with col6:
    st.metric("↩️ Page Comments", f"{cmt_reply:,}")

st.markdown("---")

# ============================================
# BY SHIFT BREAKDOWN
# ============================================
st.subheader("🕐 Performance by Shift")

cur.execute("""
    WITH msg_shift AS (
        SELECT
            is_from_page,
            conversation_id,
            CASE
                WHEN EXTRACT(HOUR FROM (message_time AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')) BETWEEN 6 AND 13 THEN 'Morning (6am-2pm)'
                WHEN EXTRACT(HOUR FROM (message_time AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')) BETWEEN 14 AND 21 THEN 'Mid (2pm-10pm)'
                ELSE 'Evening (10pm-6am)'
            END as shift
        FROM messages
        WHERE (message_time AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')::date = %s
    )
    SELECT
        shift,
        COUNT(*) FILTER (WHERE is_from_page = false) as received,
        COUNT(*) FILTER (WHERE is_from_page = true) as sent,
        COUNT(DISTINCT conversation_id) FILTER (WHERE is_from_page = false) as new_chats,
        ROUND(100.0 * COUNT(*) FILTER (WHERE is_from_page = true) / NULLIF(COUNT(*) FILTER (WHERE is_from_page = false), 0), 1) as response_rate
    FROM msg_shift
    GROUP BY shift
    ORDER BY CASE shift
        WHEN 'Morning (6am-2pm)' THEN 1
        WHEN 'Mid (2pm-10pm)' THEN 2
        ELSE 3
    END
""", (report_date,))
shift_data = cur.fetchall()

if shift_data:
    shift_df = pd.DataFrame(shift_data, columns=['Shift', 'Received', 'Sent', 'New Chats', 'Response %'])

    col1, col2 = st.columns([1, 1])

    with col1:
        # Format for display
        shift_display = shift_df.copy()
        for col in ['Received', 'Sent', 'New Chats']:
            shift_display[col] = shift_display[col].apply(format_number)
        shift_display['Response %'] = shift_display['Response %'].apply(lambda x: f"{x:.1f}%" if pd.notna(x) else "N/A")
        st.dataframe(shift_display, hide_index=True)

    with col2:
        fig = px.bar(
            shift_df,
            x='Shift',
            y=['Received', 'Sent'],
            barmode='group',
            title='Messages by Shift',
            color_discrete_map={'Received': '#3B82F6', 'Sent': '#10B981'}
        )
        fig.update_layout(height=300)
        st.plotly_chart(fig, use_container_width=True)
else:
    st.info("No shift data available")

st.markdown("---")

# ============================================
# TOP PAGES
# ============================================
st.subheader("📄 Top Pages Performance")

cur.execute("""
    SELECT
        p.page_name,
        COUNT(*) FILTER (WHERE m.is_from_page = false) as received,
        COUNT(*) FILTER (WHERE m.is_from_page = true) as sent,
        COUNT(DISTINCT m.conversation_id) FILTER (WHERE m.is_from_page = false) as new_chats
    FROM messages m
    JOIN pages p ON m.page_id = p.page_id
    WHERE (m.message_time AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')::date = %s
    GROUP BY p.page_name
    HAVING COUNT(*) FILTER (WHERE m.is_from_page = false) > 0
    ORDER BY received DESC
    LIMIT 10
""", (report_date,))
page_data = cur.fetchall()

if page_data:
    page_df = pd.DataFrame(page_data, columns=['Page', 'Received', 'Sent', 'New Chats'])

    col1, col2 = st.columns([1, 1])

    with col1:
        page_display = page_df.copy()
        for col in ['Received', 'Sent', 'New Chats']:
            page_display[col] = page_display[col].apply(format_number)
        st.dataframe(page_display, hide_index=True)

    with col2:
        fig = px.bar(
            page_df.head(10),
            x='Page',
            y='Received',
            title='Top 10 Pages by Messages Received',
            color_discrete_sequence=['#3B82F6']
        )
        fig.update_layout(height=350, xaxis_tickangle=-45)
        st.plotly_chart(fig, use_container_width=True)
else:
    st.info("No page data available")

st.markdown("---")

# ============================================
# SMA AGENT PERFORMANCE
# ============================================
st.subheader("👤 SMA Agent Performance")

cur.execute("""
    SELECT
        a.agent_name,
        s.shift,
        s.schedule_status,
        s.duty_hours,
        s.messages_received,
        s.messages_sent,
        s.comments_received,
        s.comment_replies,
        s.avg_response_time_seconds
    FROM agent_daily_stats s
    JOIN agents a ON s.agent_id = a.id
    WHERE s.date = %s
    ORDER BY a.agent_name,
        CASE s.shift WHEN 'Morning' THEN 1 WHEN 'Mid' THEN 2 ELSE 3 END
""", (report_date,))
agent_data = cur.fetchall()

if agent_data:
    agent_df = pd.DataFrame(agent_data, columns=[
        'Agent', 'Shift', 'Status', 'Hours', 'Msg Recv', 'Msg Sent',
        'Cmt Recv', 'Cmt Reply', 'Avg RT (s)'
    ])

    # Format display
    agent_display = agent_df.copy()
    for col in ['Msg Recv', 'Msg Sent', 'Cmt Recv', 'Cmt Reply']:
        agent_display[col] = agent_display[col].apply(format_number)
    agent_display['Avg RT (s)'] = agent_display['Avg RT (s)'].apply(
        lambda x: f"{x/60:.1f}m" if x and x >= 60 else (f"{x:.0f}s" if x else "N/A")
    )

    # Style status
    def style_status(val):
        if val == 'present':
            return 'background-color: #dcfce7; color: #166534'
        elif val == 'absent':
            return 'background-color: #fee2e2; color: #991b1b'
        elif val == 'off':
            return 'background-color: #fef3c7; color: #92400e'
        return ''

    styled = agent_display.style.map(style_status, subset=['Status'])
    st.dataframe(styled, hide_index=True, height=400)

    # Agent totals
    st.markdown("**Agent Totals:**")
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("📥 Total Msg Recv", f"{agent_df['Msg Recv'].sum():,}")
    with col2:
        st.metric("📤 Total Msg Sent", f"{agent_df['Msg Sent'].sum():,}")
    with col3:
        st.metric("💬 Total Cmt Recv", f"{agent_df['Cmt Recv'].sum():,}")
    with col4:
        st.metric("↩️ Total Cmt Reply", f"{agent_df['Cmt Reply'].sum():,}")
else:
    st.info("No agent data available for this date. Run aggregate_agent_stats.py to generate.")

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
    WHERE (message_time AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')::date = %s
    GROUP BY hour
    ORDER BY hour
""", (report_date,))
hourly_data = cur.fetchall()

if hourly_data:
    hourly_df = pd.DataFrame(hourly_data, columns=['Hour', 'Received', 'Sent'])

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=hourly_df['Hour'],
        y=hourly_df['Received'],
        name='Received',
        mode='lines+markers',
        line=dict(color='#3B82F6')
    ))
    fig.add_trace(go.Scatter(
        x=hourly_df['Hour'],
        y=hourly_df['Sent'],
        name='Sent',
        mode='lines+markers',
        line=dict(color='#10B981')
    ))
    fig.update_layout(
        title=f'Hourly Message Volume - {report_date.strftime("%B %d, %Y")}',
        xaxis_title='Hour (PHT)',
        yaxis_title='Messages',
        xaxis=dict(tickmode='linear', tick0=0, dtick=2),
        height=350
    )
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("No hourly data available")

cur.close()
conn.close()

# Footer
st.markdown("---")
st.caption("""
**Metric Definitions:**
- **Messages Received**: Incoming messages from users
- **Messages Sent**: Outgoing replies from page
- **Comments Received**: User comments on page posts (excludes page's own comments)
- **Page Comments**: Comments posted by the page (includes self-comments AND replies to users - API limitation)
- **Response Rate**: Messages Sent / Messages Received × 100%
""")
st.caption("All times in Philippine Time (UTC+8) | Data from Facebook Graph API")
