"""
Agent Leaderboard - Rankings and Performance Tiers
Comprehensive agent performance scoring with multi-metric analysis
"""

import streamlit as st
import pandas as pd
from datetime import date, timedelta
import plotly.express as px
import plotly.graph_objects as go

# Import shared modules
from config import CORE_PAGES, CORE_PAGES_SQL, TIMEZONE, CACHE_TTL, COLORS
from db_utils import get_simple_connection as get_connection
from utils import format_number, format_rt, format_percentage

# Page config
st.set_page_config(
    page_title="Agent Leaderboard",
    page_icon="🏆",
    layout="wide"
)

# ============================================
# SCORING CONFIGURATION
# ============================================
SCORE_WEIGHTS = {
    'response_rate': 0.30,      # 30% - Response rate percentage
    'messages_handled': 0.25,    # 25% - Total messages handled
    'response_time': 0.20,       # 20% - Average response time (inverse - lower is better)
    'attendance': 0.15,          # 15% - Attendance rate
    'efficiency': 0.10           # 10% - Efficiency (sent vs received ratio)
}

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

# ============================================
# DATA FUNCTIONS
# ============================================

@st.cache_data(ttl=CACHE_TTL["default"])
def get_agent_stats(start_date, end_date, page_filter=None):
    """Get comprehensive agent statistics for leaderboard"""
    conn = get_connection()
    cur = conn.cursor()

    # Simple query using only columns that exist in agent_daily_stats
    query = """
        WITH agent_metrics AS (
            SELECT
                a.id as agent_id,
                a.agent_name,
                -- Message metrics
                COALESCE(SUM(ads.messages_received), 0) as total_recv,
                COALESCE(SUM(ads.messages_sent), 0) as total_sent,
                -- Response time (human only, >10s)
                AVG(ads.avg_response_time_seconds) FILTER (WHERE ads.avg_response_time_seconds > 10) as avg_rt,
                -- Attendance
                COUNT(DISTINCT ads.date) FILTER (WHERE ads.schedule_status = 'present') as days_present,
                COUNT(DISTINCT ads.date) FILTER (WHERE ads.schedule_status = 'absent') as days_absent,
                COUNT(DISTINCT ads.date) FILTER (WHERE ads.schedule_status = 'off') as days_off,
                COUNT(DISTINCT ads.date) as total_days,
                -- Comment replies
                COALESCE(SUM(ads.comment_replies), 0) as comment_replies
            FROM agents a
            JOIN agent_daily_stats ads ON a.id = ads.agent_id
            WHERE ads.date BETWEEN %s AND %s
              AND a.is_active = true
            GROUP BY a.id, a.agent_name
        )
        SELECT
            agent_name,
            total_recv,
            total_sent,
            CASE WHEN total_recv > 0 THEN ROUND((100.0 * total_sent / total_recv)::numeric, 1) ELSE 0 END as response_rate,
            avg_rt,
            days_present,
            days_absent,
            days_off,
            total_days,
            CASE WHEN (days_present + days_absent) > 0
                THEN ROUND((100.0 * days_present / (days_present + days_absent))::numeric, 1)
                ELSE 0
            END as attendance_rate,
            comment_replies,
            total_recv + total_sent as total_messages
        FROM agent_metrics
        WHERE total_recv > 0 OR days_present > 0
        ORDER BY total_messages DESC
    """

    cur.execute(query, (start_date, end_date))
    rows = cur.fetchall()
    cur.close()
    conn.close()

    columns = [
        'agent_name', 'total_recv', 'total_sent', 'response_rate', 'avg_rt',
        'days_present', 'days_absent', 'days_off', 'total_days', 'attendance_rate',
        'comment_replies', 'total_messages'
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
    """Get weekly performance trend for an agent"""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT
            DATE_TRUNC('week', ads.date)::date as week_start,
            SUM(ads.messages_received) as recv,
            SUM(ads.messages_sent) as sent,
            CASE WHEN SUM(ads.messages_received) > 0
                THEN ROUND((100.0 * SUM(ads.messages_sent) / SUM(ads.messages_received))::numeric, 1)
                ELSE 0
            END as response_rate,
            AVG(ads.avg_response_time_seconds) FILTER (WHERE ads.avg_response_time_seconds > 10) as avg_rt
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
    return pd.DataFrame(rows, columns=['week', 'recv', 'sent', 'response_rate', 'avg_rt'])


# ============================================
# SCORING FUNCTIONS
# ============================================

def calculate_performance_score(row, max_values):
    """Calculate weighted performance score for an agent (0-100)"""

    # Convert all values to float to handle Decimal types from database
    response_rate = float(row['response_rate'] or 0)
    total_messages = float(row['total_messages'] or 0)
    total_recv = float(row['total_recv'] or 0)
    total_sent = float(row['total_sent'] or 0)
    avg_rt = float(row['avg_rt'] or 0)
    attendance_rate = float(row['attendance_rate'] or 0)

    # Response Rate Score (0-100, capped at 100%)
    rr_score = min(response_rate, 100)

    # Messages Handled Score (normalized to max)
    max_messages = float(max_values.get('total_messages', 1) or 1)
    msg_score = (total_messages / max_messages) * 100 if max_messages > 0 else 0

    # Response Time Score (inverse - lower is better)
    # Target: <5min=100, 5-15min=75, 15-30min=50, 30-60min=25, >60min=0
    if avg_rt <= 300:  # 5 minutes
        rt_score = 100
    elif avg_rt <= 900:  # 15 minutes
        rt_score = 100 - ((avg_rt - 300) / 600) * 25
    elif avg_rt <= 1800:  # 30 minutes
        rt_score = 75 - ((avg_rt - 900) / 900) * 25
    elif avg_rt <= 3600:  # 60 minutes
        rt_score = 50 - ((avg_rt - 1800) / 1800) * 25
    else:
        rt_score = max(0, 25 - ((avg_rt - 3600) / 3600) * 25)

    # Attendance Score
    att_score = attendance_rate

    # Efficiency Score (sent/received ratio, capped at 100%)
    if total_recv > 0:
        eff_score = min((total_sent / total_recv) * 100, 100)
    else:
        eff_score = 0

    # Weighted total
    total_score = (
        SCORE_WEIGHTS['response_rate'] * rr_score +
        SCORE_WEIGHTS['messages_handled'] * msg_score +
        SCORE_WEIGHTS['response_time'] * rt_score +
        SCORE_WEIGHTS['attendance'] * att_score +
        SCORE_WEIGHTS['efficiency'] * eff_score
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

    # Scoring info
    st.subheader("Scoring Weights")
    for metric, weight in SCORE_WEIGHTS.items():
        st.caption(f"• {metric.replace('_', ' ').title()}: {int(weight*100)}%")

    st.markdown("---")

    # Tier thresholds
    st.subheader("Tier Thresholds")
    for tier, threshold in TIER_THRESHOLDS.items():
        badge = TIER_BADGES.get(tier, '')
        st.caption(f"{badge} {tier.title()}: {threshold}+ pts")

# Get data
agent_stats = get_agent_stats(start_date, end_date)

if agent_stats.empty:
    st.warning("No agent data available for the selected period")
    st.stop()

# Calculate scores and tiers
max_values = {
    'total_messages': agent_stats['total_messages'].max()
}

agent_stats['score'] = agent_stats.apply(lambda row: calculate_performance_score(row, max_values), axis=1)
agent_stats['tier'] = agent_stats['score'].apply(get_tier)
agent_stats['rank'] = agent_stats['score'].rank(ascending=False, method='min').astype(int)

# Sort by score
agent_stats = agent_stats.sort_values('score', ascending=False).reset_index(drop=True)

# ============================================
# SUMMARY STATS
# ============================================
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric("Total Agents", len(agent_stats))

with col2:
    platinum_gold = len(agent_stats[agent_stats['tier'].isin(['platinum', 'gold'])])
    st.metric("Top Performers", f"{platinum_gold} agents", help="Platinum + Gold tier agents")

with col3:
    avg_score = agent_stats['score'].mean()
    st.metric("Avg Score", f"{avg_score:.1f}")

with col4:
    avg_response = agent_stats['response_rate'].mean()
    st.metric("Avg Response Rate", f"{avg_response:.1f}%")

st.markdown("---")

# ============================================
# LEADERBOARD TABS
# ============================================
tab1, tab2, tab3, tab4 = st.tabs(["Overall Rankings", "By Tier", "By Response Time", "Agent Details"])

with tab1:
    st.subheader("Overall Performance Rankings")

    # Create display dataframe
    display_df = agent_stats.copy()

    # Add tier badges
    display_df['Tier'] = display_df['tier'].apply(lambda x: f"{TIER_BADGES.get(x, '')} {x.title()}")
    display_df['Rank'] = display_df['rank'].apply(lambda x: f"#{x}")

    # Format columns
    display_df['Score'] = display_df['score'].apply(lambda x: f"{x:.1f}")
    display_df['Response Rate'] = display_df['response_rate'].apply(lambda x: f"{x:.1f}%")
    display_df['Avg RT'] = display_df['avg_rt'].apply(format_rt)
    display_df['Messages'] = display_df['total_messages'].apply(lambda x: f"{int(x):,}")
    display_df['Attendance'] = display_df['attendance_rate'].apply(lambda x: f"{x:.1f}%")

    # Select and rename columns
    leaderboard = display_df[[
        'Rank', 'agent_name', 'Score', 'Tier', 'Messages', 'Response Rate', 'Avg RT', 'Attendance'
    ]].rename(columns={'agent_name': 'Agent'})

    st.dataframe(
        leaderboard,
        hide_index=True,
        width="stretch",
        height=min(len(leaderboard) * 35 + 38, 600)
    )

    # Score distribution chart
    st.subheader("Score Distribution")

    fig = px.histogram(
        agent_stats,
        x='score',
        nbins=20,
        title='Agent Score Distribution',
        labels={'score': 'Performance Score', 'count': 'Number of Agents'},
        color_discrete_sequence=[COLORS['primary']]
    )

    # Add tier threshold lines
    for tier, threshold in TIER_THRESHOLDS.items():
        badge, color = get_tier_display(tier)
        fig.add_vline(x=threshold, line_dash="dash", line_color=color,
                      annotation_text=f"{tier.title()}", annotation_position="top")

    fig.update_layout(height=300, margin=dict(l=0, r=0, t=40, b=0))
    st.plotly_chart(fig, width="stretch")

with tab2:
    st.subheader("Agents by Performance Tier")

    # Group by tier
    tier_order = ['platinum', 'gold', 'silver', 'bronze', 'standard']

    for tier in tier_order:
        tier_agents = agent_stats[agent_stats['tier'] == tier]
        if not tier_agents.empty:
            badge, color = get_tier_display(tier)

            with st.expander(f"{badge} {tier.title()} Tier ({len(tier_agents)} agents)", expanded=(tier in ['platinum', 'gold'])):
                tier_display = tier_agents[[
                    'rank', 'agent_name', 'score', 'total_messages', 'response_rate', 'avg_rt', 'attendance_rate'
                ]].copy()

                tier_display['rank'] = tier_display['rank'].apply(lambda x: f"#{x}")
                tier_display['score'] = tier_display['score'].apply(lambda x: f"{x:.1f}")
                tier_display['total_messages'] = tier_display['total_messages'].apply(lambda x: f"{int(x):,}")
                tier_display['response_rate'] = tier_display['response_rate'].apply(lambda x: f"{x:.1f}%")
                tier_display['avg_rt'] = tier_display['avg_rt'].apply(format_rt)
                tier_display['attendance_rate'] = tier_display['attendance_rate'].apply(lambda x: f"{x:.1f}%")

                tier_display.columns = ['Rank', 'Agent', 'Score', 'Messages', 'Response Rate', 'Avg RT', 'Attendance']

                st.dataframe(tier_display, hide_index=True, width="stretch")

with tab3:
    st.subheader("Response Time Leaderboard")

    # Filter agents with valid response time
    rt_agents = agent_stats[agent_stats['avg_rt'].notna() & (agent_stats['avg_rt'] > 0)].copy()
    rt_agents = rt_agents.sort_values('avg_rt', ascending=True).reset_index(drop=True)
    rt_agents['rt_rank'] = range(1, len(rt_agents) + 1)

    col1, col2 = st.columns([2, 1])

    with col1:
        rt_display = rt_agents[[
            'rt_rank', 'agent_name', 'avg_rt', 'response_rate', 'total_messages'
        ]].copy()

        rt_display['rt_rank'] = rt_display['rt_rank'].apply(lambda x: f"#{x}")
        rt_display['avg_rt'] = rt_display['avg_rt'].apply(format_rt)
        rt_display['response_rate'] = rt_display['response_rate'].apply(lambda x: f"{x:.1f}%")
        rt_display['total_messages'] = rt_display['total_messages'].apply(lambda x: f"{int(x):,}")

        rt_display.columns = ['Rank', 'Agent', 'Avg Response Time', 'Response Rate', 'Messages']

        st.dataframe(rt_display.head(20), hide_index=True, width="stretch")

    with col2:
        # Response time distribution
        fig = px.box(
            agent_stats[agent_stats['avg_rt'].notna()],
            y='avg_rt',
            title='Response Time Distribution',
            labels={'avg_rt': 'Seconds'},
            color_discrete_sequence=[COLORS['secondary']]
        )
        fig.update_layout(height=400, margin=dict(l=0, r=0, t=40, b=0))
        st.plotly_chart(fig, width="stretch")

with tab4:
    st.subheader("Agent Performance Details")

    # Agent selector
    selected_agent = st.selectbox(
        "Select Agent",
        options=agent_stats['agent_name'].tolist(),
        format_func=lambda x: f"#{agent_stats[agent_stats['agent_name']==x]['rank'].values[0]} - {x}"
    )

    if selected_agent:
        agent_row = agent_stats[agent_stats['agent_name'] == selected_agent].iloc[0]
        badge, color = get_tier_display(agent_row['tier'])

        # Header with tier badge
        st.markdown(f"### {badge} {selected_agent}")
        st.markdown(f"**Rank:** #{int(agent_row['rank'])} | **Score:** {agent_row['score']:.1f} | **Tier:** {agent_row['tier'].title()}")

        st.markdown("---")

        # Metrics grid
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.metric("Messages Handled", f"{int(agent_row['total_messages']):,}")
            st.caption(f"Received: {int(agent_row['total_recv']):,}")
            st.caption(f"Sent: {int(agent_row['total_sent']):,}")

        with col2:
            st.metric("Response Rate", f"{agent_row['response_rate']:.1f}%")

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
        max_msg = float(max_values.get('total_messages', 1))
        rr_score = min(float(agent_row['response_rate'] or 0), 100)
        msg_score = (float(agent_row['total_messages'] or 0) / max_msg) * 100 if max_msg > 0 else 0

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

        total_recv = float(agent_row['total_recv'] or 0)
        total_sent = float(agent_row['total_sent'] or 0)
        if total_recv > 0:
            eff_score = min((total_sent / total_recv) * 100, 100)
        else:
            eff_score = 0

        score_data = pd.DataFrame({
            'Metric': ['Response Rate', 'Message Volume', 'Response Time', 'Attendance', 'Efficiency'],
            'Score': [rr_score, msg_score, rt_score, att_score, eff_score],
            'Weight': [30, 25, 20, 15, 10],
            'Weighted': [
                rr_score * 0.30,
                msg_score * 0.25,
                rt_score * 0.20,
                att_score * 0.15,
                eff_score * 0.10
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
                    mode='lines+markers',
                    line=dict(color=COLORS['primary'], width=2)
                ))
                fig.add_trace(go.Scatter(
                    x=trend_data['week'],
                    y=trend_data['sent'],
                    name='Sent',
                    mode='lines+markers',
                    line=dict(color=COLORS['secondary'], width=2)
                ))
                fig.update_layout(
                    title='Weekly Messages',
                    height=250,
                    margin=dict(l=0, r=0, t=40, b=0),
                    legend=dict(orientation="h", yanchor="bottom", y=1.02)
                )
                st.plotly_chart(fig, width="stretch")

            with col2:
                fig = px.line(
                    trend_data,
                    x='week',
                    y='response_rate',
                    title='Weekly Response Rate',
                    markers=True
                )
                fig.update_traces(line_color=COLORS['accent'])
                fig.update_layout(height=250, margin=dict(l=0, r=0, t=40, b=0))
                fig.update_yaxes(title='Response Rate (%)')
                st.plotly_chart(fig, width="stretch")
        else:
            st.info("No weekly trend data available for this agent")

# ============================================
# EXPORT
# ============================================
st.markdown("---")

with st.expander("Export Leaderboard"):
    # Prepare export data
    export_df = agent_stats[[
        'rank', 'agent_name', 'score', 'tier', 'total_recv', 'total_sent',
        'response_rate', 'avg_rt', 'attendance_rate', 'days_present', 'days_absent'
    ]].copy()

    export_df.columns = [
        'Rank', 'Agent', 'Score', 'Tier', 'Messages Received', 'Messages Sent',
        'Response Rate %', 'Avg Response Time (s)', 'Attendance Rate %', 'Days Present', 'Days Absent'
    ]

    csv = export_df.to_csv(index=False)

    col1, col2 = st.columns(2)
    with col1:
        st.download_button(
            label="Download CSV",
            data=csv,
            file_name=f"leaderboard_{start_date}_to_{end_date}.csv",
            mime="text/csv"
        )
    with col2:
        st.caption(f"Export includes {len(export_df)} agents")

# Footer
st.markdown("---")
st.caption("All times in Philippine Time (UTC+8) | Scoring based on multi-metric weighted analysis")
