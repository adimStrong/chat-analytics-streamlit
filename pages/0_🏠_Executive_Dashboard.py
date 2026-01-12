"""
Executive Dashboard - High-level KPIs and Performance Overview
Management summary with period comparison and alerts
"""

import streamlit as st
import pandas as pd
from datetime import date, timedelta
import plotly.express as px
import plotly.graph_objects as go

# Import shared modules
from config import (
    CORE_PAGES, CORE_PAGES_SQL, TIMEZONE, CACHE_TTL, COLORS,
    LIVESTREAM_PAGES_SQL, SOCMED_PAGES_SQL,
    QA_WEIGHTS, QA_RESPONSE_THRESHOLDS, SPILL_KEYWORDS, SPILL_START_DATE
)
from db_utils import get_simple_connection as get_connection
from utils import format_number, format_rt

# Page config
st.set_page_config(
    page_title="Executive Dashboard",
    page_icon="🏠",
    layout="wide"
)

# ============================================
# DATA FUNCTIONS
# ============================================

@st.cache_data(ttl=CACHE_TTL["default"])
def get_period_metrics(start_date, end_date, page_filter_sql):
    """Get key metrics for a date period with page filter"""
    conn = get_connection()
    cur = conn.cursor()

    # Messages metrics
    cur.execute("""
        WITH first_messages AS (
            SELECT sender_id, MIN(message_time) as first_msg_time
            FROM messages m
            JOIN pages p ON m.page_id = p.page_id
            WHERE is_from_page = false AND p.page_name IN %s
            GROUP BY sender_id
        )
        SELECT
            COUNT(*) FILTER (WHERE m.is_from_page = false) as msg_recv,
            COUNT(*) FILTER (WHERE m.is_from_page = true) as msg_sent,
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
    """, (page_filter_sql, start_date, end_date, start_date, end_date, page_filter_sql))
    msg_row = cur.fetchone()

    # Comments metrics
    cur.execute("""
        SELECT
            COUNT(*) FILTER (WHERE c.author_id IS NULL OR c.author_id != c.page_id) as cmt_recv,
            COUNT(*) FILTER (WHERE c.author_id IS NOT NULL AND c.author_id = c.page_id) as cmt_reply
        FROM comments c
        JOIN pages p ON c.page_id = p.page_id
        WHERE (c.comment_time AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')::date BETWEEN %s AND %s
          AND p.page_name IN %s
    """, (start_date, end_date, page_filter_sql))
    cmt_row = cur.fetchone()

    # Response time from messages (fallback to sessions if no message RT data)
    cur.execute("""
        SELECT
            AVG(m.response_time_seconds) FILTER (WHERE m.response_time_seconds > 10) as avg_human_rt,
            COUNT(DISTINCT m.conversation_id) as unique_convos
        FROM messages m
        JOIN pages p ON m.page_id = p.page_id
        WHERE (m.message_time AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')::date BETWEEN %s AND %s
          AND p.page_name IN %s
          AND m.is_from_page = true
          AND m.response_time_seconds > 0
    """, (start_date, end_date, page_filter_sql))
    msg_rt_row = cur.fetchone()

    # Fallback to sessions if messages have no response time data
    if msg_rt_row[0] is None:
        cur.execute("""
            SELECT
                AVG(s.avg_response_time_seconds) FILTER (WHERE s.avg_response_time_seconds > 10) as avg_human_rt,
                COUNT(DISTINCT s.conversation_id) as unique_convos
            FROM sessions s
            JOIN pages p ON s.page_id = p.page_id
            WHERE (s.session_start AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')::date BETWEEN %s AND %s
              AND p.page_name IN %s
              AND s.avg_response_time_seconds > 0
        """, (start_date, end_date, page_filter_sql))
        session_row = cur.fetchone()
    else:
        session_row = msg_rt_row

    cur.close()
    conn.close()

    msg_recv = msg_row[0] or 0 if msg_row else 0
    msg_sent = msg_row[1] or 0 if msg_row else 0

    return {
        'msg_recv': msg_recv,
        'msg_sent': msg_sent,
        'unique_users': msg_row[2] or 0 if msg_row else 0,
        'new_chats': msg_row[3] or 0 if msg_row else 0,
        'cmt_recv': cmt_row[0] or 0 if cmt_row else 0,
        'cmt_reply': cmt_row[1] or 0 if cmt_row else 0,
        'avg_human_rt': session_row[0] or 0 if session_row else 0,
        'unique_convos': session_row[1] or 0 if session_row else 0,
        'response_rate': round(100 * msg_sent / msg_recv, 1) if msg_recv > 0 else 0
    }

@st.cache_data(ttl=CACHE_TTL["default"])
def get_daily_trend(start_date, end_date, page_filter_sql):
    """Get daily message trend for chart with page filter"""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT
            (m.message_time AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')::date as date,
            COUNT(*) FILTER (WHERE m.is_from_page = false) as received,
            COUNT(*) FILTER (WHERE m.is_from_page = true) as sent
        FROM messages m
        JOIN pages p ON m.page_id = p.page_id
        WHERE (m.message_time AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')::date BETWEEN %s AND %s
          AND p.page_name IN %s
        GROUP BY date
        ORDER BY date
    """, (start_date, end_date, page_filter_sql))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return pd.DataFrame(rows, columns=['date', 'received', 'sent'])

@st.cache_data(ttl=CACHE_TTL["default"])
def get_agent_alerts(start_date, end_date):
    """Get agents with performance issues for alerts"""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT
            a.agent_name,
            ads.date,
            ads.shift,
            ads.schedule_status,
            ads.messages_received,
            ads.messages_sent,
            CASE
                WHEN ads.messages_received > 0
                THEN ROUND((100.0 * ads.messages_sent / ads.messages_received)::numeric, 1)
                ELSE 0
            END as response_rate,
            ads.avg_response_time_seconds
        FROM agents a
        JOIN agent_daily_stats ads ON a.id = ads.agent_id
        WHERE ads.date BETWEEN %s AND %s
          AND a.is_active = true
          AND (
              ads.schedule_status = 'absent'
              OR (ads.messages_received > 10 AND ads.messages_sent::float / NULLIF(ads.messages_received, 0) < 0.5)
              OR ads.avg_response_time_seconds > 1800
          )
        ORDER BY ads.date DESC, a.agent_name
    """, (start_date, end_date))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return pd.DataFrame(rows, columns=['Agent', 'Date', 'Shift', 'Status', 'Msg Recv', 'Msg Sent', 'Response %', 'Avg RT'])

def calculate_response_time_score(avg_rt_seconds):
    """Calculate response time score based on thresholds"""
    if avg_rt_seconds is None or avg_rt_seconds <= 0:
        return 50.0  # Default score when no data
    for tier, config in QA_RESPONSE_THRESHOLDS.items():
        if avg_rt_seconds <= config['max_seconds']:
            return float(config['score'])
    return 20.0  # Poor score for very slow responses

def calculate_productivity_score(unique_users, avg_unique_users):
    """Calculate productivity score relative to team average"""
    if avg_unique_users <= 0:
        return 50.0
    score = (unique_users / avg_unique_users) * 100
    return min(100.0, score)

def build_spill_sql_conditions():
    """Build SQL OR conditions for spill keyword detection"""
    conditions = []
    for keyword in SPILL_KEYWORDS:
        # Escape single quotes for SQL and lowercase
        # Use %% to escape % for psycopg2 parameter substitution
        escaped = keyword.replace("'", "''").lower()
        conditions.append(f"LOWER(m.message_text) LIKE '%%{escaped}%%'")
    return " OR ".join(conditions)

@st.cache_data(ttl=CACHE_TTL["default"])
def get_top_performers(start_date, end_date, page_filter_sql, limit=5):
    """Get top performing agents by QA Score"""
    from datetime import datetime

    conn = get_connection()
    cur = conn.cursor()

    # Get base agent stats (only count when present)
    cur.execute("""
        SELECT
            a.agent_name,
            a.id as agent_id,
            SUM(ads.messages_received) FILTER (WHERE ads.schedule_status = 'present') as total_recv,
            SUM(ads.messages_sent) FILTER (WHERE ads.schedule_status = 'present') as total_sent,
            AVG(ads.avg_response_time_seconds) FILTER (WHERE ads.avg_response_time_seconds > 10 AND ads.schedule_status = 'present') as avg_rt,
            COUNT(DISTINCT ads.date) FILTER (WHERE ads.schedule_status = 'present') as days_present
        FROM agents a
        JOIN agent_daily_stats ads ON a.id = ads.agent_id
        WHERE ads.date BETWEEN %s AND %s
          AND a.is_active = true
        GROUP BY a.agent_name, a.id
        HAVING SUM(ads.messages_received) FILTER (WHERE ads.schedule_status = 'present') > 0
    """, (start_date, end_date))
    agent_rows = cur.fetchall()

    if not agent_rows:
        cur.close()
        conn.close()
        return pd.DataFrame(columns=['Agent', 'QA Score', 'Msg Recv', 'Msg Sent', 'Avg RT', 'Days Present'])

    # Calculate team average messages for productivity scoring (using messages_sent as proxy)
    total_sent = sum(row[3] or 0 for row in agent_rows)
    avg_messages = total_sent / len(agent_rows) if agent_rows else 1

    # Get resolution rates (spill detection) if date range includes spill tracking period
    spill_start = datetime.strptime(SPILL_START_DATE, "%Y-%m-%d").date()
    resolution_rates = {}

    if end_date >= spill_start:
        effective_start = max(start_date, spill_start)
        spill_conditions = build_spill_sql_conditions()

        cur.execute(f"""
            SELECT
                a.agent_name,
                COUNT(DISTINCT c.conversation_id) as total_convos,
                COUNT(DISTINCT CASE WHEN ({spill_conditions}) THEN c.conversation_id END) as resolved_convos
            FROM agents a
            JOIN agent_page_assignments apa ON a.id = apa.agent_id
            JOIN pages p ON apa.page_id = p.page_id
            JOIN conversations c ON p.page_id = c.page_id
            JOIN messages m ON c.conversation_id = m.conversation_id
            WHERE c.last_message_time >= %s AND c.last_message_time < %s + INTERVAL '1 day'
              AND p.page_name IN %s
              AND m.is_from_page = true
              AND a.is_active = true
            GROUP BY a.agent_name
        """, (effective_start, end_date, page_filter_sql))

        for row in cur.fetchall():
            agent_name, total_convos, resolved_convos = row
            if total_convos and total_convos > 0:
                resolution_rates[agent_name] = (resolved_convos / total_convos) * 100
            else:
                resolution_rates[agent_name] = 50.0

    # Calculate QA scores for each agent
    results = []
    for row in agent_rows:
        agent_name, agent_id, total_recv, total_sent, avg_rt, days_present = row

        # Response Time Score (40%)
        rt_score = calculate_response_time_score(avg_rt)

        # Resolution Rate (35%)
        res_rate = resolution_rates.get(agent_name, 50.0)

        # Productivity Score (25%) - based on messages sent relative to team average
        prod_score = calculate_productivity_score(total_sent or 0, avg_messages)

        # Combined QA Score
        qa_score = (
            QA_WEIGHTS['response_time'] * rt_score +
            QA_WEIGHTS['resolution_rate'] * res_rate +
            QA_WEIGHTS['productivity'] * prod_score
        )

        results.append({
            'Agent': agent_name,
            'QA Score': round(qa_score, 1),
            'Msg Recv': total_recv or 0,
            'Msg Sent': total_sent or 0,
            'Avg RT': avg_rt,
            'Days Present': days_present or 0
        })

    cur.close()
    conn.close()

    # Sort by QA Score descending and limit
    df = pd.DataFrame(results)
    df = df.sort_values('QA Score', ascending=False).head(limit)
    return df

# ============================================
# HELPER FUNCTIONS
# ============================================

def calculate_change(current, previous):
    """Calculate percentage change between two values"""
    if previous == 0:
        return 0 if current == 0 else 100
    return round(((current - previous) / previous) * 100, 1)

def get_change_color(change, higher_is_better=True):
    """Get color for change indicator"""
    if change > 0:
        return "green" if higher_is_better else "red"
    elif change < 0:
        return "red" if higher_is_better else "green"
    return "gray"

def display_metric_with_comparison(label, current, previous, format_func=None, higher_is_better=True, suffix=""):
    """Display a metric with comparison to previous period"""
    change = calculate_change(current, previous)

    if format_func:
        display_value = format_func(current)
    else:
        display_value = f"{current:,}{suffix}"

    delta = f"{change:+.1f}%" if change != 0 else "0%"
    delta_color = "normal" if higher_is_better else "inverse"

    st.metric(label, display_value, delta, delta_color=delta_color)

# ============================================
# MAIN APP
# ============================================

# Get page filter name for display
page_filter_display = st.session_state.get('page_filter_name', 'All Pages')

# Logo and Title
col_logo, col_title = st.columns([0.08, 0.92])
with col_logo:
    st.image("Juan365.jpg", width=60)
with col_title:
    st.title("Executive Dashboard")
    st.caption(f"Performance overview ({page_filter_display}) | Generated: {date.today().strftime('%B %d, %Y')}")

st.markdown("---")

# Sidebar - Date Selection
with st.sidebar:
    st.header("Report Settings")

    # Period selection
    period_options = {
        "Yesterday": 1,
        "Last 7 Days": 7,
        "Last 14 Days": 14,
        "Last 30 Days": 30
    }

    selected_period = st.selectbox("Select Period", list(period_options.keys()), index=1)
    days = period_options[selected_period]

    today = date.today()
    end_date = today - timedelta(days=1)  # T+1
    start_date = end_date - timedelta(days=days-1)

    # Previous period for comparison
    prev_end_date = start_date - timedelta(days=1)
    prev_start_date = prev_end_date - timedelta(days=days-1)

    st.caption(f"Current: {start_date} to {end_date}")
    st.caption(f"Previous: {prev_start_date} to {prev_end_date}")

    st.markdown("---")

    # Show filtered pages info
    page_filter_name = st.session_state.get('page_filter_name', 'All Pages')
    st.subheader(f"Showing: {page_filter_name}")

    # Get the page list based on filter
    if page_filter_name == "Live Stream":
        page_list = list(LIVESTREAM_PAGES_SQL)
    elif page_filter_name == "Socmed":
        page_list = list(SOCMED_PAGES_SQL)
    else:
        page_list = CORE_PAGES

    for page in page_list:
        st.caption(f"- {page}")

# Get page filter from session state
page_filter_sql = st.session_state.get('page_filter_sql', CORE_PAGES_SQL)

# Get data for both periods
current_metrics = get_period_metrics(start_date, end_date, page_filter_sql)
previous_metrics = get_period_metrics(prev_start_date, prev_end_date, page_filter_sql)

# ============================================
# KPI CARDS WITH COMPARISON
# ============================================
st.subheader("Key Performance Indicators")
st.caption(f"Comparing {selected_period} vs Previous {days} Days")

col1, col2, col3, col4 = st.columns(4)

with col1:
    change = calculate_change(current_metrics['msg_recv'], previous_metrics['msg_recv'])
    st.metric(
        "Messages Received",
        f"{current_metrics['msg_recv']:,}",
        f"{change:+.1f}%"
    )

with col2:
    change = calculate_change(current_metrics['msg_sent'], previous_metrics['msg_sent'])
    st.metric(
        "Messages Sent",
        f"{current_metrics['msg_sent']:,}",
        f"{change:+.1f}%"
    )

with col3:
    change = calculate_change(current_metrics['response_rate'], previous_metrics['response_rate'])
    st.metric(
        "Response Rate",
        f"{current_metrics['response_rate']:.1f}%",
        f"{change:+.1f}%"
    )

with col4:
    current_rt = current_metrics['avg_human_rt']
    previous_rt = previous_metrics['avg_human_rt']
    change = calculate_change(current_rt, previous_rt)
    rt_display = format_rt(current_rt) if current_rt else "N/A"
    st.metric(
        "Avg Response Time",
        rt_display,
        f"{change:+.1f}%" if current_rt else None,
        delta_color="inverse"  # Lower is better
    )

# Second row of KPIs
col1, col2, col3, col4 = st.columns(4)

with col1:
    change = calculate_change(current_metrics['new_chats'], previous_metrics['new_chats'])
    st.metric(
        "New Chats",
        f"{current_metrics['new_chats']:,}",
        f"{change:+.1f}%"
    )

with col2:
    change = calculate_change(current_metrics['unique_users'], previous_metrics['unique_users'])
    st.metric(
        "Unique Users",
        f"{current_metrics['unique_users']:,}",
        f"{change:+.1f}%"
    )

with col3:
    change = calculate_change(current_metrics['cmt_recv'], previous_metrics['cmt_recv'])
    st.metric(
        "Comments Received",
        f"{current_metrics['cmt_recv']:,}",
        f"{change:+.1f}%"
    )

with col4:
    change = calculate_change(current_metrics['cmt_reply'], previous_metrics['cmt_reply'])
    st.metric(
        "Comment Replies",
        f"{current_metrics['cmt_reply']:,}",
        f"{change:+.1f}%"
    )

st.markdown("---")

# ============================================
# TREND CHART AND ALERTS
# ============================================
col_chart, col_alerts = st.columns([2, 1])

with col_chart:
    st.subheader("Message Trend")

    trend_data = get_daily_trend(start_date, end_date, page_filter_sql)

    if not trend_data.empty:
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=trend_data['date'],
            y=trend_data['received'],
            name='Received',
            mode='lines+markers',
            line=dict(color=COLORS['primary'], width=2),
            fill='tozeroy',
            fillcolor='rgba(59, 130, 246, 0.1)'
        ))
        fig.add_trace(go.Scatter(
            x=trend_data['date'],
            y=trend_data['sent'],
            name='Sent',
            mode='lines+markers',
            line=dict(color=COLORS['secondary'], width=2)
        ))
        fig.update_layout(
            height=300,
            margin=dict(l=0, r=0, t=10, b=0),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            xaxis_title="",
            yaxis_title="Messages"
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No message data for selected period")

with col_alerts:
    st.subheader("Alerts")

    alerts = get_agent_alerts(start_date, end_date)

    if not alerts.empty:
        # Count alert types
        absent_count = len(alerts[alerts['Status'] == 'absent'])
        low_response = len(alerts[alerts['Response %'] < 50])
        slow_response = len(alerts[alerts['Avg RT'] > 1800])

        if absent_count > 0:
            st.error(f"**{absent_count}** agent absence(s) recorded")

        if low_response > 0:
            st.warning(f"**{low_response}** instance(s) of low response rate (<50%)")

        if slow_response > 0:
            st.warning(f"**{slow_response}** instance(s) of slow response (>30min)")

        with st.expander("View Details", expanded=False):
            alerts_display = alerts.copy()
            alerts_display['Date'] = pd.to_datetime(alerts_display['Date']).dt.strftime('%Y-%m-%d')
            alerts_display['Avg RT'] = alerts_display['Avg RT'].apply(format_rt)
            st.dataframe(alerts_display, hide_index=True, height=200)
    else:
        st.success("No alerts - all metrics within normal range")

st.markdown("---")

# ============================================
# TOP PERFORMERS (by QA Score)
# ============================================
st.subheader("Top Performers by QA Score")

top_agents = get_top_performers(start_date, end_date, page_filter_sql)

if not top_agents.empty:
    col1, col2 = st.columns([2, 1])

    with col1:
        # Add rank and medal
        top_display = top_agents.copy()
        medals = ['🥇', '🥈', '🥉', '4th', '5th']
        top_display.insert(0, 'Rank', [medals[i] if i < 3 else medals[i] for i in range(len(top_display))])
        top_display['Avg RT'] = top_display['Avg RT'].apply(format_rt)
        top_display['QA Score'] = top_display['QA Score'].apply(lambda x: f"{x:.1f}")

        for col in ['Msg Recv', 'Msg Sent', 'Days Present']:
            top_display[col] = top_display[col].apply(lambda x: f"{int(x):,}" if pd.notna(x) else "0")

        st.dataframe(top_display, hide_index=True, use_container_width=True)

    with col2:
        # Bar chart of top performers by QA Score
        fig = px.bar(
            top_agents.head(5),
            x='Agent',
            y='QA Score',
            title='QA Score by Agent',
            color='QA Score',
            color_continuous_scale=['#ef4444', '#f59e0b', '#10b981']
        )
        fig.update_layout(height=250, showlegend=False, margin=dict(l=0, r=0, t=30, b=0))
        fig.update_coloraxes(showscale=False)
        st.plotly_chart(fig, use_container_width=True)
else:
    st.info("No agent performance data for selected period")

# ============================================
# QUICK LINKS
# ============================================
st.markdown("---")
st.subheader("Quick Links")

col1, col2 = st.columns(2)

with col1:
    st.page_link("pages/2_📅_T1_Report.py", label="📅 T+1 Daily Report", icon="📅")

with col2:
    st.page_link("pages/3_🏆_Leaderboard.py", label="🏆 Agent Leaderboard", icon="🏆")

# Footer
st.markdown("---")
st.caption("All times in Philippine Time (UTC+8) | Data from Facebook Graph API")
