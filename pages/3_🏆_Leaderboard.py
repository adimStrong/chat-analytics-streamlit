"""
Agent Leaderboard - Rankings and Performance Tiers
Comprehensive agent performance scoring with multi-metric analysis

QA Scoring System (Industry Standard):
- Response Time Score: 40% (how fast agents respond)
- Resolution Rate: 35% (% of conversations closed with spill keywords)
- Productivity Score: 25% (messages sent per day vs team average)
"""

import streamlit as st
import pandas as pd
from datetime import date, timedelta, datetime
import plotly.express as px
import plotly.graph_objects as go

# Import shared modules
from config import (
    CORE_PAGES, CORE_PAGES_SQL, TIMEZONE, CACHE_TTL, COLORS,
    LIVESTREAM_PAGES_SQL, SOCMED_PAGES_SQL,
    SPILL_KEYWORDS, SPILL_START_DATE, QA_WEIGHTS, QA_RESPONSE_THRESHOLDS
)
from db_utils import get_simple_connection as get_connection
from utils import format_number, format_rt, format_percentage

# Page config
st.set_page_config(
    page_title="Agent Leaderboard",
    page_icon="🏆",
    layout="wide"
)

# ============================================
# SCORING CONFIGURATION (from config.py)
# ============================================
# Uses QA_WEIGHTS from config:
#   response_time: 40% - How fast agents respond
#   resolution_rate: 35% - % of conversations closed with spill keywords
#   productivity: 25% - Messages sent per day vs team average

TIER_THRESHOLDS = {
    'platinum': 90,
    'gold': 75,
    'silver': 60,
    'bronze': 45
}

TIER_BADGES = {
    'platinum': '💎',
    'gold': '🥇',
    'silver': '🥈',
    'bronze': '🥉',
    'standard': '📊'
}

# Build spill detection SQL conditions
def build_spill_sql_conditions():
    """Build SQL LIKE conditions for spill detection"""
    conditions = []
    for keyword in SPILL_KEYWORDS:
        conditions.append(f"LOWER(message_text) LIKE '%{keyword.lower()}%'")
    return " OR ".join(conditions)

# ============================================
# DATA FUNCTIONS
# ============================================

@st.cache_data(ttl=CACHE_TTL["default"])
def get_agent_stats(start_date, end_date):
    """Get comprehensive agent statistics for leaderboard"""
    conn = get_connection()
    cur = conn.cursor()

    # Query with unique users (unique conversations)
    query = """
        WITH agent_metrics AS (
            SELECT
                a.id as agent_id,
                a.agent_name,
                -- Message metrics (only count when present)
                COALESCE(SUM(ads.messages_received) FILTER (WHERE ads.schedule_status = 'present'), 0) as total_recv,
                COALESCE(SUM(ads.messages_sent) FILTER (WHERE ads.schedule_status = 'present'), 0) as total_sent,
                -- Response time (human only, >10s, only when present)
                AVG(ads.avg_response_time_seconds) FILTER (WHERE ads.avg_response_time_seconds > 10 AND ads.schedule_status = 'present') as avg_rt,
                -- Attendance
                COUNT(DISTINCT ads.date) FILTER (WHERE ads.schedule_status = 'present') as days_present,
                COUNT(DISTINCT ads.date) FILTER (WHERE ads.schedule_status = 'absent') as days_absent,
                COUNT(DISTINCT ads.date) FILTER (WHERE ads.schedule_status = 'off') as days_off,
                COUNT(DISTINCT ads.date) as total_days,
                -- Comment replies (only count when present)
                COALESCE(SUM(ads.comment_replies) FILTER (WHERE ads.schedule_status = 'present'), 0) as comment_replies
            FROM agents a
            JOIN agent_daily_stats ads ON a.id = ads.agent_id
            WHERE ads.date BETWEEN %s AND %s
              AND a.is_active = true
            GROUP BY a.id, a.agent_name
        ),
        unique_users AS (
            SELECT
                a.agent_name,
                COUNT(DISTINCT c.participant_id) as unique_user_count
            FROM agents a
            JOIN agent_page_assignments apa ON a.id = apa.agent_id
            JOIN conversations c ON c.page_id = apa.page_id
            WHERE c.updated_time::date BETWEEN %s AND %s
              AND a.is_active = true
            GROUP BY a.agent_name
        )
        SELECT
            am.agent_name,
            am.total_recv,
            am.total_sent,
            am.avg_rt,
            am.days_present,
            am.days_absent,
            am.days_off,
            am.total_days,
            CASE WHEN (am.days_present + am.days_absent) > 0
                THEN ROUND((100.0 * am.days_present / (am.days_present + am.days_absent))::numeric, 1)
                ELSE 0
            END as attendance_rate,
            am.comment_replies,
            COALESCE(uu.unique_user_count, 0) as unique_users
        FROM agent_metrics am
        LEFT JOIN unique_users uu ON am.agent_name = uu.agent_name
        WHERE am.total_recv > 0 OR am.days_present > 0
        ORDER BY unique_user_count DESC NULLS LAST
    """

    cur.execute(query, (start_date, end_date, start_date, end_date))
    rows = cur.fetchall()
    cur.close()
    conn.close()

    columns = [
        'agent_name', 'total_recv', 'total_sent', 'avg_rt',
        'days_present', 'days_absent', 'days_off', 'total_days', 'attendance_rate',
        'comment_replies', 'unique_users'
    ]

    return pd.DataFrame(rows, columns=columns)


@st.cache_data(ttl=CACHE_TTL["date_range"])
def get_date_range():
    """Get available date range from data"""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT MIN(date), MAX(date)
        FROM agent_daily_stats
    """)
    row = cur.fetchone()
    cur.close()
    conn.close()
    return row[0], row[1] if row else (None, None)


@st.cache_data(ttl=CACHE_TTL["default"])
def get_weekly_trend(start_date, end_date, agent_name):
    """Get weekly performance trend for an agent (only counts data when present)"""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT
            DATE_TRUNC('week', ads.date)::date as week_start,
            SUM(ads.messages_received) FILTER (WHERE ads.schedule_status = 'present') as recv,
            SUM(ads.messages_sent) FILTER (WHERE ads.schedule_status = 'present') as sent,
            AVG(ads.avg_response_time_seconds) FILTER (WHERE ads.avg_response_time_seconds > 10 AND ads.schedule_status = 'present') as avg_rt
        FROM agents a
        JOIN agent_daily_stats ads ON a.id = ads.agent_id
        WHERE a.agent_name = %s
          AND ads.date BETWEEN %s AND %s
        GROUP BY week_start
        ORDER BY week_start
    """, (agent_name, start_date, end_date))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return pd.DataFrame(rows, columns=['week', 'recv', 'sent', 'avg_rt'])


@st.cache_data(ttl=CACHE_TTL["default"])
def get_resolution_rates(start_date, end_date, page_filter_sql):
    """
    Get resolution rates by agent based on spill detection.
    Resolution = conversation has a message with spill keywords from page.
    Only counts data from SPILL_START_DATE onwards.
    """
    # Check if date range includes spill tracking period
    spill_start = datetime.strptime(SPILL_START_DATE, "%Y-%m-%d").date()

    # Adjust start date if before spill tracking
    effective_start = max(start_date, spill_start) if isinstance(start_date, date) else start_date

    if effective_start > end_date:
        # No spill data available for this date range
        return pd.DataFrame(columns=['agent_name', 'total_conversations', 'resolved_conversations', 'resolution_rate'])

    conn = get_connection()
    cur = conn.cursor()

    # Build spill conditions
    spill_conditions = build_spill_sql_conditions()

    query = f"""
        WITH agent_conversations AS (
            -- Get conversations handled by each agent (based on page assignments)
            SELECT
                a.agent_name,
                c.conversation_id,
                c.page_id
            FROM agents a
            JOIN agent_page_assignments apa ON a.id = apa.agent_id AND apa.is_active = true
            JOIN pages p ON apa.page_id = p.page_id
            JOIN conversations c ON c.page_id = apa.page_id
            WHERE a.is_active = true
              AND p.page_name IN %s
              AND c.updated_time::date BETWEEN %s AND %s
        ),
        resolved_convos AS (
            -- Find conversations with spill messages from page
            SELECT DISTINCT
                ac.agent_name,
                ac.conversation_id
            FROM agent_conversations ac
            JOIN messages m ON m.conversation_id = ac.conversation_id
            WHERE m.is_from_page = true
              AND m.message_time::date BETWEEN %s AND %s
              AND ({spill_conditions})
        )
        SELECT
            ac.agent_name,
            COUNT(DISTINCT ac.conversation_id) as total_conversations,
            COUNT(DISTINCT rc.conversation_id) as resolved_conversations
        FROM agent_conversations ac
        LEFT JOIN resolved_convos rc ON ac.agent_name = rc.agent_name
                                     AND ac.conversation_id = rc.conversation_id
        GROUP BY ac.agent_name
    """

    cur.execute(query, (page_filter_sql, effective_start, end_date, effective_start, end_date))
    rows = cur.fetchall()
    cur.close()
    conn.close()

    df = pd.DataFrame(rows, columns=['agent_name', 'total_conversations', 'resolved_conversations'])

    # Calculate resolution rate
    df['resolution_rate'] = df.apply(
        lambda row: (row['resolved_conversations'] / row['total_conversations'] * 100)
        if row['total_conversations'] > 0 else 0,
        axis=1
    )

    return df


# ============================================
# SCORING FUNCTIONS
# ============================================

def calculate_response_time_score(avg_rt):
    """
    Calculate Response Time Score based on thresholds (40% of total)
    Uses QA_RESPONSE_THRESHOLDS from config.py
    """
    if avg_rt is None or avg_rt <= 0:
        return 50  # Default if no data

    avg_rt = float(avg_rt)

    # Check against thresholds from config
    if avg_rt <= QA_RESPONSE_THRESHOLDS['excellent']['max_seconds']:
        return 100
    elif avg_rt <= QA_RESPONSE_THRESHOLDS['good']['max_seconds']:
        # Linear interpolation from 100 to 80
        range_size = QA_RESPONSE_THRESHOLDS['good']['max_seconds'] - QA_RESPONSE_THRESHOLDS['excellent']['max_seconds']
        return 100 - ((avg_rt - 300) / range_size) * 20
    elif avg_rt <= QA_RESPONSE_THRESHOLDS['average']['max_seconds']:
        # Linear interpolation from 80 to 60
        range_size = QA_RESPONSE_THRESHOLDS['average']['max_seconds'] - QA_RESPONSE_THRESHOLDS['good']['max_seconds']
        return 80 - ((avg_rt - 900) / range_size) * 20
    elif avg_rt <= QA_RESPONSE_THRESHOLDS['below_average']['max_seconds']:
        # Linear interpolation from 60 to 40
        range_size = QA_RESPONSE_THRESHOLDS['below_average']['max_seconds'] - QA_RESPONSE_THRESHOLDS['average']['max_seconds']
        return 60 - ((avg_rt - 1800) / range_size) * 20
    else:
        # Below 40, approaching 20
        return max(20, 40 - ((avg_rt - 3600) / 3600) * 20)


def calculate_qa_score(avg_rt):
    """
    Legacy QA Score function - now wraps calculate_response_time_score
    Kept for backward compatibility with display
    """
    return calculate_response_time_score(avg_rt)


def get_qa_rating(qa_score):
    """Get QA rating label based on score"""
    if qa_score >= 90:
        return "Excellent", "🟢"
    elif qa_score >= 75:
        return "Good", "🟡"
    elif qa_score >= 50:
        return "Average", "🟠"
    else:
        return "Needs Improvement", "🔴"


def calculate_productivity_score(msgs_sent_per_day, avg_msgs_per_day):
    """
    Calculate Productivity Score based on messages sent per day vs team average.
    This measures actual individual work done, not shared page stats.
    Score = (agent_daily_msgs / team_avg_daily) * 100, capped at 100
    """
    if avg_msgs_per_day <= 0:
        return 50  # Default if no baseline
    return min(100, (msgs_sent_per_day / avg_msgs_per_day) * 100)


def calculate_performance_score(row, max_values, resolution_rate=None):
    """
    Calculate weighted performance score for an agent (0-100)
    Uses new industry-standard QA formula:
    - Response Time Score: 40%
    - Resolution Rate: 35% (spill detection)
    - Productivity Score: 25% (msgs sent per day vs team avg)
    """

    # Convert all values to float to handle Decimal types from database
    avg_rt = float(row['avg_rt'] or 0)
    total_sent = float(row['total_sent'] or 0)
    days_present = float(row['days_present'] or 1)

    # 1. Response Time Score (40%)
    rt_score = calculate_response_time_score(avg_rt)

    # 2. Resolution Rate (35%) - from spill detection
    # If resolution_rate is None (before spill tracking started), use neutral 50%
    res_rate = float(resolution_rate) if resolution_rate is not None else 50.0

    # 3. Productivity Score (25%) - messages sent per day vs team average
    # This measures actual individual work, not shared page stats
    msgs_per_day = total_sent / max(days_present, 1)
    avg_msgs_per_day = float(max_values.get('avg_msgs_per_day', 1) or 1)
    prod_score = calculate_productivity_score(msgs_per_day, avg_msgs_per_day)

    # Weighted total using QA_WEIGHTS from config
    total_score = (
        QA_WEIGHTS['response_time'] * rt_score +
        QA_WEIGHTS['resolution_rate'] * res_rate +
        QA_WEIGHTS['productivity'] * prod_score
    )

    return round(float(total_score), 1)


def get_tier(score):
    """Get performance tier based on score"""
    if score >= TIER_THRESHOLDS['platinum']:
        return 'platinum'
    elif score >= TIER_THRESHOLDS['gold']:
        return 'gold'
    elif score >= TIER_THRESHOLDS['silver']:
        return 'silver'
    elif score >= TIER_THRESHOLDS['bronze']:
        return 'bronze'
    return 'standard'


def get_tier_display(tier):
    """Get badge and color for tier"""
    badge = TIER_BADGES.get(tier, '📊')
    colors = {
        'platinum': '#E5E4E2',
        'gold': '#FFD700',
        'silver': '#C0C0C0',
        'bronze': '#CD7F32',
        'standard': '#808080'
    }
    return badge, colors.get(tier, '#808080')


# ============================================
# MAIN APP
# ============================================

# Logo and Title
col_logo, col_title = st.columns([0.08, 0.92])
with col_logo:
    st.image("Juan365.jpg", width=60)
with col_title:
    st.title("Agent Leaderboard")
    st.caption(f"Performance rankings and tier system | Generated: {date.today().strftime('%B %d, %Y')}")

st.markdown("---")

# Sidebar - Filters
with st.sidebar:
    st.header("Leaderboard Settings")

    # Get date range
    min_date, max_date = get_date_range()
    if min_date is None:
        st.error("No data available")
        st.stop()

    # Period selection
    period_options = {
        "Last 7 Days": 7,
        "Last 14 Days": 14,
        "Last 30 Days": 30,
        "Last 60 Days": 60,
        "Custom Range": 0
    }

    selected_period = st.selectbox("Select Period", list(period_options.keys()), index=2)

    if selected_period == "Custom Range":
        col1, col2 = st.columns(2)
        with col1:
            start_date = st.date_input("Start", min_date, min_value=min_date, max_value=max_date)
        with col2:
            end_date = st.date_input("End", max_date, min_value=min_date, max_value=max_date)
    else:
        days = period_options[selected_period]
        end_date = max_date
        start_date = end_date - timedelta(days=days-1)

    st.caption(f"Period: {start_date} to {end_date}")

    st.markdown("---")

    # Scoring info (using QA_WEIGHTS from config)
    st.subheader("QA Scoring Formula")
    st.markdown(f"""
    - **Response Time**: {int(QA_WEIGHTS['response_time']*100)}% - Speed of replies
    - **Resolution Rate**: {int(QA_WEIGHTS['resolution_rate']*100)}% - Proper conversation closings
    - **Productivity**: {int(QA_WEIGHTS['productivity']*100)}% - Messages sent per day vs team avg
    """)
    st.caption("Productivity = (Your Msgs/Day ÷ Team Avg) × 100")

    st.markdown("---")

    st.subheader("Tier Thresholds")
    st.markdown(f"""
    - 💎 Platinum: {TIER_THRESHOLDS['platinum']}+
    - 🥇 Gold: {TIER_THRESHOLDS['gold']}+
    - 🥈 Silver: {TIER_THRESHOLDS['silver']}+
    - 🥉 Bronze: {TIER_THRESHOLDS['bronze']}+
    """)

# Get page filter from session state (set in app.py)
page_filter_sql = st.session_state.get('page_filter_sql', CORE_PAGES_SQL)
page_filter_name = st.session_state.get('page_filter_name', 'All Pages')

# Show current filter
st.info(f"Showing data for: **{page_filter_name}**")

# Load data
df = get_agent_stats(start_date, end_date)

if df.empty:
    st.warning("No agent data found for the selected period.")
    st.stop()

# Load resolution rates (spill detection)
resolution_df = get_resolution_rates(start_date, end_date, page_filter_sql)

# Check if we're in spill tracking period
spill_start = datetime.strptime(SPILL_START_DATE, "%Y-%m-%d").date()
is_spill_period = end_date >= spill_start

# Merge resolution rates with agent data
if not resolution_df.empty and is_spill_period:
    df = df.merge(resolution_df[['agent_name', 'resolution_rate', 'resolved_conversations', 'total_conversations']],
                  on='agent_name', how='left')
    df['resolution_rate'] = df['resolution_rate'].fillna(0)
else:
    # Before spill tracking: use None so calculate_performance_score uses 50% neutral
    df['resolution_rate'] = None
    df['resolved_conversations'] = 0
    df['total_conversations'] = 0

if not is_spill_period:
    st.info(f"Spill tracking starts {SPILL_START_DATE}. Resolution scores use neutral 50% for earlier dates.")

# Calculate messages per day for productivity scoring
df['msgs_per_day'] = df.apply(
    lambda row: float(row['total_sent'] or 0) / max(float(row['days_present'] or 1), 1),
    axis=1
)

# Calculate max values for normalization
max_values = {
    'unique_users': df['unique_users'].max() if 'unique_users' in df.columns else 1,
    'avg_unique_users': df['unique_users'].mean() if 'unique_users' in df.columns else 1,
    'avg_msgs_per_day': df['msgs_per_day'].mean() if 'msgs_per_day' in df.columns else 1
}

# Calculate scores and tiers
df['qa_score'] = df.apply(lambda row: calculate_qa_score(row['avg_rt']), axis=1)
df['rt_score'] = df.apply(lambda row: calculate_response_time_score(row['avg_rt']), axis=1)
df['productivity_score'] = df.apply(
    lambda row: calculate_productivity_score(float(row['msgs_per_day'] or 0), max_values['avg_msgs_per_day']),
    axis=1
)
df['score'] = df.apply(
    lambda row: calculate_performance_score(row, max_values, row.get('resolution_rate')),
    axis=1
)
df['tier'] = df['score'].apply(get_tier)

# Sort by score
df = df.sort_values('score', ascending=False).reset_index(drop=True)
df['rank'] = df.index + 1

# Summary stats
st.subheader("📊 Summary")
col1, col2, col3, col4, col5 = st.columns(5)

with col1:
    st.metric("Total Agents", len(df))

with col2:
    top_performers = len(df[df['tier'].isin(['platinum', 'gold'])])
    st.metric("Top Performers", top_performers, help="Platinum + Gold tier agents")

with col3:
    avg_score = df['score'].mean()
    st.metric("Avg Score", f"{avg_score:.1f}")

with col4:
    avg_rt = df['rt_score'].mean()
    st.metric("Avg RT Score", f"{avg_rt:.1f}", help="Response Time Score (40%)")

with col5:
    # Handle None resolution values (before spill tracking)
    valid_res = df['resolution_rate'].dropna()
    if not valid_res.empty:
        avg_resolution = valid_res.mean()
        st.metric("Avg Resolution", f"{avg_resolution:.1f}%", help="Resolution Rate from Spill (35%)")
    else:
        st.metric("Avg Resolution", "N/A", help="Spill tracking starts " + SPILL_START_DATE)

st.markdown("---")

# Tabs
tab1, tab2, tab3, tab4 = st.tabs(["🏆 Overall Rankings", "📊 By Tier", "⏱️ By Response Time", "👤 Agent Details"])

with tab1:
    st.subheader("Overall Rankings")

    # Create display dataframe with new metrics
    display_df = df[['rank', 'agent_name', 'score', 'tier', 'rt_score', 'resolution_rate', 'productivity_score', 'msgs_per_day', 'avg_rt']].copy()
    display_df['tier_badge'] = display_df['tier'].apply(lambda x: TIER_BADGES.get(x, '📊'))
    display_df['avg_rt_display'] = display_df['avg_rt'].apply(lambda x: format_rt(x) if x else 'N/A')
    display_df['msgs_per_day_display'] = display_df['msgs_per_day'].apply(lambda x: f"{x:.1f}" if x else '0')
    # Handle None resolution_rate (before spill tracking)
    display_df['resolution_display'] = display_df['resolution_rate'].apply(lambda x: f"{x:.1f}%" if x is not None else 'N/A')

    # Format for display
    display_df = display_df.rename(columns={
        'rank': 'Rank',
        'agent_name': 'Agent',
        'score': 'Score',
        'tier_badge': 'Tier',
        'rt_score': 'RT Score',
        'resolution_display': 'Resolution',
        'productivity_score': 'Productivity',
        'msgs_per_day_display': 'Msgs/Day',
        'avg_rt_display': 'Avg RT'
    })

    display_cols = ['Rank', 'Tier', 'Agent', 'Score', 'RT Score', 'Resolution', 'Productivity', 'Msgs/Day', 'Avg RT']
    st.dataframe(
        display_df[display_cols],
        hide_index=True,
        width="stretch",
        column_config={
            "Score": st.column_config.NumberColumn(format="%.1f"),
            "RT Score": st.column_config.NumberColumn(format="%.1f"),
            "Resolution %": st.column_config.NumberColumn(format="%.1f"),
            "Productivity": st.column_config.NumberColumn(format="%.1f"),
            "Unique Users": st.column_config.NumberColumn(format="%d")
        }
    )

    # Score distribution chart
    st.subheader("Score Distribution")
    fig = px.histogram(
        df,
        x='score',
        nbins=20,
        title='Agent Score Distribution',
        color_discrete_sequence=[COLORS['primary']]
    )
    fig.update_layout(
        xaxis_title="Score",
        yaxis_title="Number of Agents",
        height=300
    )
    st.plotly_chart(fig, width="stretch")

with tab2:
    st.subheader("Agents by Tier")

    tier_order = ['platinum', 'gold', 'silver', 'bronze', 'standard']

    for tier in tier_order:
        tier_df = df[df['tier'] == tier]
        if len(tier_df) > 0:
            badge, color = get_tier_display(tier)
            with st.expander(f"{badge} {tier.title()} ({len(tier_df)} agents)", expanded=(tier in ['platinum', 'gold'])):
                tier_display = tier_df[['rank', 'agent_name', 'score', 'qa_score', 'unique_users', 'attendance_rate']].copy()
                tier_display.columns = ['Rank', 'Agent', 'Score', 'QA Score', 'Unique Users', 'Attendance %']
                st.dataframe(tier_display, hide_index=True, width="stretch")

with tab3:
    st.subheader("Response Time Analysis")

    # Sort by response time
    rt_df = df.sort_values('avg_rt').copy()
    rt_df['avg_rt_display'] = rt_df['avg_rt'].apply(lambda x: format_rt(x) if x else 'N/A')
    rt_df['qa_rating'], rt_df['qa_icon'] = zip(*rt_df['qa_score'].apply(get_qa_rating))

    rt_display = rt_df[['rank', 'agent_name', 'avg_rt_display', 'qa_score', 'qa_rating', 'unique_users']].copy()
    rt_display.columns = ['Rank', 'Agent', 'Avg Response Time', 'QA Score', 'QA Rating', 'Unique Users']

    st.dataframe(rt_display.head(20), hide_index=True, width="stretch")

    # Response time chart
    st.subheader("Response Time by Agent")

    # Convert avg_rt to numeric and filter out None/NaN values for chart
    df_with_rt = df[df['avg_rt'].notna()].copy()
    df_with_rt['avg_rt'] = pd.to_numeric(df_with_rt['avg_rt'], errors='coerce')
    chart_df = df_with_rt.nsmallest(15, 'avg_rt').copy()
    chart_df['avg_rt_min'] = chart_df['avg_rt'] / 60  # Convert to minutes

    fig = px.bar(
        chart_df,
        x='agent_name',
        y='avg_rt_min',
        title='Top 15 Fastest Response Times',
        color='qa_score',
        color_continuous_scale=['#ef4444', '#f59e0b', '#10b981'],
        range_color=[0, 100]
    )
    fig.update_layout(
        xaxis_title="Agent",
        yaxis_title="Avg Response Time (minutes)",
        height=400
    )
    fig.update_coloraxes(colorbar_title="QA Score")
    st.plotly_chart(fig, width="stretch")

with tab4:
    st.subheader("Agent Details")

    # Agent selector
    agent_names = df['agent_name'].tolist()
    selected_agent = st.selectbox("Select Agent", agent_names)

    if selected_agent:
        agent_row = df[df['agent_name'] == selected_agent].iloc[0]
        tier = agent_row['tier']
        badge, color = get_tier_display(tier)
        qa_rating, qa_icon = get_qa_rating(agent_row['qa_score'])

        # Header with tier badge
        st.markdown(f"### {badge} {selected_agent}")
        st.markdown(f"**Rank:** #{int(agent_row['rank'])} | **Score:** {agent_row['score']:.1f} | **Tier:** {tier.title()}")

        st.markdown("---")

        # Metrics grid
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.metric("QA Score", f"{agent_row['qa_score']:.1f}")
            st.caption(f"{qa_icon} {qa_rating}")

        with col2:
            st.metric("Unique Users", f"{int(agent_row['unique_users']):,}")

        with col3:
            st.metric("Avg Response Time", format_rt(agent_row['avg_rt']))

        with col4:
            st.metric("Attendance Rate", f"{agent_row['attendance_rate']:.1f}%")
            st.caption(f"Present: {int(agent_row['days_present'])} days")
            if agent_row['days_absent'] > 0:
                st.caption(f"Absent: {int(agent_row['days_absent'])} days")

        # Score breakdown
        st.subheader("Score Breakdown")

        # Calculate individual scores (convert to float for Decimal compatibility)
        max_users = float(max_values.get('unique_users', 1))
        qa_score = float(agent_row['qa_score'] or 0)
        unique_users = float(agent_row['unique_users'] or 0)
        users_score = (unique_users / max_users) * 100 if max_users > 0 else 0

        avg_rt = float(agent_row['avg_rt'] or 0)
        if avg_rt <= 300:
            rt_score = 100
        elif avg_rt <= 900:
            rt_score = 100 - ((avg_rt - 300) / 600) * 25
        elif avg_rt <= 1800:
            rt_score = 75 - ((avg_rt - 900) / 900) * 25
        elif avg_rt <= 3600:
            rt_score = 50 - ((avg_rt - 1800) / 1800) * 25
        else:
            rt_score = max(0, 25 - ((avg_rt - 3600) / 3600) * 25)

        att_score = float(agent_row['attendance_rate'] or 0)

        score_data = pd.DataFrame({
            'Metric': ['QA Score', 'Unique Users', 'Response Time', 'Attendance'],
            'Score': [qa_score, users_score, rt_score, att_score],
            'Weight': [35, 30, 20, 15],
            'Weighted': [
                qa_score * 0.35,
                users_score * 0.30,
                rt_score * 0.20,
                att_score * 0.15
            ]
        })

        col1, col2 = st.columns([1, 1])

        with col1:
            score_display = score_data.copy()
            score_display['Score'] = score_display['Score'].apply(lambda x: f"{x:.1f}")
            score_display['Weight'] = score_display['Weight'].apply(lambda x: f"{x}%")
            score_display['Weighted'] = score_display['Weighted'].apply(lambda x: f"{x:.1f}")
            st.dataframe(score_display, hide_index=True, width="stretch")

        with col2:
            fig = px.bar(
                score_data,
                x='Score',
                y='Metric',
                orientation='h',
                title='Score by Metric (0-100)',
                color='Score',
                color_continuous_scale=['#ef4444', '#f59e0b', '#10b981'],
                range_color=[0, 100]
            )
            fig.update_layout(height=250, showlegend=False, margin=dict(l=0, r=0, t=40, b=0))
            fig.update_coloraxes(showscale=False)
            st.plotly_chart(fig, width="stretch")

        # Weekly trend
        st.subheader("Weekly Performance Trend")

        trend_data = get_weekly_trend(start_date, end_date, selected_agent)

        if not trend_data.empty:
            col1, col2 = st.columns(2)

            with col1:
                fig = go.Figure()
                fig.add_trace(go.Scatter(
                    x=trend_data['week'],
                    y=trend_data['recv'],
                    name='Received',
                    line=dict(color=COLORS['primary'])
                ))
                fig.add_trace(go.Scatter(
                    x=trend_data['week'],
                    y=trend_data['sent'],
                    name='Sent',
                    line=dict(color=COLORS['secondary'])
                ))
                fig.update_layout(
                    title='Messages by Week',
                    height=250,
                    margin=dict(l=0, r=0, t=40, b=0)
                )
                st.plotly_chart(fig, width="stretch")

            with col2:
                trend_data['avg_rt_min'] = trend_data['avg_rt'] / 60
                fig = px.line(
                    trend_data,
                    x='week',
                    y='avg_rt_min',
                    title='Response Time by Week (minutes)',
                    markers=True
                )
                fig.update_traces(line_color=COLORS['accent'])
                fig.update_layout(height=250, margin=dict(l=0, r=0, t=40, b=0))
                st.plotly_chart(fig, width="stretch")
        else:
            st.info("No weekly trend data available for this period.")
