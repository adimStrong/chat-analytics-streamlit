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
st.title("📊 T+1 Daily Report")
if use_date_range:
    st.markdown(f"### Report Period: **{date_label}**")
else:
    st.markdown(f"### Report Date: **{date_label}**")
st.caption(f"Generated on: {today.strftime('%B %d, %Y')} | All times in Philippine Time (UTC+8)")

st.markdown("---")

# ============================================
# SUMMARY METRICS
# ============================================
conn = get_connection()
cur = conn.cursor()

# Messages summary with unique users
cur.execute("""
    SELECT
        COUNT(*) FILTER (WHERE is_from_page = false) as recv,
        COUNT(*) FILTER (WHERE is_from_page = true) as sent,
        COUNT(DISTINCT conversation_id) FILTER (WHERE is_from_page = false) as new_convos,
        COUNT(DISTINCT sender_id) FILTER (WHERE is_from_page = false) as unique_users
    FROM messages
    WHERE (message_time AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')::date BETWEEN %s AND %s
""", (from_date, to_date))
msg_row = cur.fetchone()
msg_recv, msg_sent, new_convos, unique_users = msg_row if msg_row else (0, 0, 0, 0)

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
    st.metric("💬 New Conversations", f"{new_convos:,}")
with col5:
    st.metric("📊 Response Rate", f"{response_rate:.1f}%")
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
            sender_id,
            CASE
                WHEN EXTRACT(HOUR FROM (message_time AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')) BETWEEN 6 AND 13 THEN 'Morning (6am-2pm)'
                WHEN EXTRACT(HOUR FROM (message_time AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')) BETWEEN 14 AND 21 THEN 'Mid (2pm-10pm)'
                ELSE 'Evening (10pm-6am)'
            END as shift
        FROM messages
        WHERE (message_time AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')::date BETWEEN %s AND %s
    )
    SELECT
        shift,
        COUNT(*) FILTER (WHERE is_from_page = false) as received,
        COUNT(*) FILTER (WHERE is_from_page = true) as sent,
        COUNT(DISTINCT conversation_id) FILTER (WHERE is_from_page = false) as new_chats,
        COUNT(DISTINCT sender_id) FILTER (WHERE is_from_page = false) as unique_users,
        ROUND(100.0 * COUNT(*) FILTER (WHERE is_from_page = true) / NULLIF(COUNT(*) FILTER (WHERE is_from_page = false), 0), 1) as response_rate
    FROM msg_shift
    GROUP BY shift
    ORDER BY CASE shift
        WHEN 'Morning (6am-2pm)' THEN 1
        WHEN 'Mid (2pm-10pm)' THEN 2
        ELSE 3
    END
""", (from_date, to_date))
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
    SELECT
        p.page_name,
        COUNT(*) FILTER (WHERE m.is_from_page = false) as received,
        COUNT(*) FILTER (WHERE m.is_from_page = true) as sent,
        COUNT(DISTINCT m.conversation_id) FILTER (WHERE m.is_from_page = false) as new_chats,
        COUNT(DISTINCT m.sender_id) FILTER (WHERE m.is_from_page = false) as unique_users
    FROM messages m
    JOIN pages p ON m.page_id = p.page_id
    WHERE (m.message_time AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')::date BETWEEN %s AND %s
    GROUP BY p.page_name
    HAVING COUNT(*) FILTER (WHERE m.is_from_page = false) > 0
    ORDER BY received DESC
    LIMIT 10
""", (from_date, to_date))
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
    'Metric': ['Messages Received', 'Messages Sent', 'Unique Users', 'New Conversations', 'Response Rate', 'Page Comments'],
    'Value': [msg_recv, msg_sent, unique_users, new_convos, f"{response_rate:.1f}%", cmt_reply]
}
export_df = pd.DataFrame(export_data)

csv_buffer = io.StringIO()
csv_buffer.write(f"T+1 Daily Report - {date_label}\n")
csv_buffer.write(f"Generated on: {today.strftime('%B %d, %Y')}\n\n")
csv_buffer.write("SUMMARY\n")
export_df.to_csv(csv_buffer, index=False)
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

with st.sidebar:
    filename = f"T1_Report_{from_date.strftime('%Y%m%d')}"
    if use_date_range:
        filename += f"_to_{to_date.strftime('%Y%m%d')}"
    filename += ".csv"

    st.download_button(
        label="📥 Download Summary CSV",
        data=csv_data,
        file_name=filename,
        mime="text/csv"
    )
    st.caption("Export includes summary, shift breakdown, top pages, and hourly trend.")

cur.close()

# Footer
st.markdown("---")
st.caption("""
**Metric Definitions:**
- **Messages Received**: Incoming messages from users
- **Messages Sent**: Outgoing replies from page
- **Unique Users**: Distinct users who sent messages
- **New Conversations**: New conversation threads started
- **Page Comments**: Comments posted by the page (replies to users)
- **Response Rate**: Messages Sent / Messages Received x 100%
""")
st.caption("All times in Philippine Time (UTC+8) | Data from Facebook Graph API")

