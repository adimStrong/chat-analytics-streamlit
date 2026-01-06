"""
Chat Analytics Dashboard - Streamlit
Direct PostgreSQL connection, no JSON exports needed
"""

import streamlit as st
import pandas as pd
import psycopg2
from datetime import datetime, timedelta
import plotly.express as px
import plotly.graph_objects as go
from dotenv import load_dotenv
import os

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

st.set_page_config(
    page_title="Chat Analytics Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 20px;
        border-radius: 10px;
        color: white;
        text-align: center;
    }
    .stMetric > div {
        background-color: #f0f2f6;
        padding: 10px;
        border-radius: 8px;
    }
</style>
""", unsafe_allow_html=True)


@st.cache_resource
def get_connection():
    """Create database connection"""
    return psycopg2.connect(DATABASE_URL)


@st.cache_data(ttl=300)
def get_pages():
    """Get all pages"""
    conn = get_connection()
    df = pd.read_sql("""
        SELECT page_id, page_name FROM pages ORDER BY page_name
    """, conn)
    return df


@st.cache_data(ttl=60)
def get_overview_stats(page_filter=None):
    """Get overview statistics"""
    conn = get_connection()

    page_clause = ""
    if page_filter and page_filter != "All Pages":
        page_clause = f"AND m.page_id = (SELECT page_id FROM pages WHERE page_name = '{page_filter}')"

    # Messages stats
    messages_df = pd.read_sql(f"""
        SELECT
            COUNT(*) as total_messages,
            COUNT(*) FILTER (WHERE is_from_page = false) as received,
            COUNT(*) FILTER (WHERE is_from_page = true) as sent
        FROM messages m
        WHERE m.message_time >= '2025-06-01'
        {page_clause}
    """, conn)

    # Comments stats
    page_clause_c = page_clause.replace("m.page_id", "c.page_id")
    comments_df = pd.read_sql(f"""
        SELECT
            COUNT(*) as total_comments,
            COUNT(DISTINCT author_id) as unique_commenters
        FROM comments c
        WHERE c.comment_time >= '2025-06-01'
        {page_clause_c}
    """, conn)

    # Sessions stats
    page_clause_s = page_clause.replace("m.page_id", "s.page_id")
    sessions_df = pd.read_sql(f"""
        SELECT
            COUNT(*) as total_sessions,
            AVG(duration_seconds) as avg_duration,
            AVG(avg_response_time_seconds) as avg_response_time
        FROM sessions s
        WHERE s.session_start >= '2025-06-01'
        {page_clause_s}
    """, conn)

    return {
        "messages": messages_df.iloc[0].to_dict(),
        "comments": comments_df.iloc[0].to_dict(),
        "sessions": sessions_df.iloc[0].to_dict()
    }


@st.cache_data(ttl=60)
def get_daily_data(page_filter=None, days=30):
    """Get daily message/comment trends"""
    conn = get_connection()

    page_clause = ""
    if page_filter and page_filter != "All Pages":
        page_clause = f"AND page_id = (SELECT page_id FROM pages WHERE page_name = '{page_filter}')"

    # Daily messages
    messages_df = pd.read_sql(f"""
        SELECT
            (message_time AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')::date as date,
            COUNT(*) as total_messages,
            COUNT(*) FILTER (WHERE is_from_page = false) as received,
            COUNT(*) FILTER (WHERE is_from_page = true) as sent
        FROM messages
        WHERE message_time >= NOW() - INTERVAL '{days} days'
        {page_clause}
        GROUP BY date
        ORDER BY date
    """, conn)

    # Daily comments
    comments_df = pd.read_sql(f"""
        SELECT
            (comment_time AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')::date as date,
            COUNT(*) as total_comments,
            COUNT(DISTINCT author_id) as unique_commenters
        FROM comments
        WHERE comment_time >= NOW() - INTERVAL '{days} days'
        {page_clause}
        GROUP BY date
        ORDER BY date
    """, conn)

    return messages_df, comments_df


@st.cache_data(ttl=60)
def get_weekly_data(page_filter=None, weeks=12):
    """Get weekly trends"""
    conn = get_connection()

    page_clause = ""
    if page_filter and page_filter != "All Pages":
        page_clause = f"AND page_id = (SELECT page_id FROM pages WHERE page_name = '{page_filter}')"

    messages_df = pd.read_sql(f"""
        SELECT
            DATE_TRUNC('week', (message_time AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila'))::date as week_start,
            COUNT(*) as total_messages,
            COUNT(*) FILTER (WHERE is_from_page = false) as received,
            COUNT(*) FILTER (WHERE is_from_page = true) as sent
        FROM messages
        WHERE message_time >= '2025-06-01'
        {page_clause}
        GROUP BY week_start
        ORDER BY week_start DESC
        LIMIT {weeks}
    """, conn)

    comments_df = pd.read_sql(f"""
        SELECT
            DATE_TRUNC('week', (comment_time AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila'))::date as week_start,
            COUNT(*) as total_comments,
            COUNT(DISTINCT author_id) as unique_commenters
        FROM comments
        WHERE comment_time >= '2025-06-01'
        {page_clause}
        GROUP BY week_start
        ORDER BY week_start DESC
        LIMIT {weeks}
    """, conn)

    return messages_df, comments_df


@st.cache_data(ttl=60)
def get_monthly_data(page_filter=None):
    """Get monthly trends"""
    conn = get_connection()

    page_clause = ""
    if page_filter and page_filter != "All Pages":
        page_clause = f"AND page_id = (SELECT page_id FROM pages WHERE page_name = '{page_filter}')"

    messages_df = pd.read_sql(f"""
        SELECT
            DATE_TRUNC('month', (message_time AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila'))::date as month,
            TO_CHAR(DATE_TRUNC('month', (message_time AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')), 'Mon YYYY') as month_name,
            COUNT(*) as total_messages,
            COUNT(*) FILTER (WHERE is_from_page = false) as received,
            COUNT(*) FILTER (WHERE is_from_page = true) as sent
        FROM messages
        WHERE message_time >= '2025-06-01'
        {page_clause}
        GROUP BY month, month_name
        ORDER BY month DESC
    """, conn)

    comments_df = pd.read_sql(f"""
        SELECT
            DATE_TRUNC('month', (comment_time AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila'))::date as month,
            TO_CHAR(DATE_TRUNC('month', (comment_time AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')), 'Mon YYYY') as month_name,
            COUNT(*) as total_comments,
            COUNT(DISTINCT author_id) as unique_commenters
        FROM comments
        WHERE comment_time >= '2025-06-01'
        {page_clause}
        GROUP BY month, month_name
        ORDER BY month DESC
    """, conn)

    return messages_df, comments_df


@st.cache_data(ttl=60)
def get_shift_data(page_filter=None):
    """Get shift breakdown"""
    conn = get_connection()

    page_clause = ""
    if page_filter and page_filter != "All Pages":
        page_clause = f"AND page_id = (SELECT page_id FROM pages WHERE page_name = '{page_filter}')"

    messages_df = pd.read_sql(f"""
        WITH shifts AS (
            SELECT
                CASE
                    WHEN EXTRACT(HOUR FROM (message_time AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')) BETWEEN 6 AND 13 THEN 'Morning (6am-2pm)'
                    WHEN EXTRACT(HOUR FROM (message_time AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')) BETWEEN 14 AND 21 THEN 'Mid (2pm-10pm)'
                    ELSE 'Evening (10pm-6am)'
                END as shift,
                is_from_page
            FROM messages
            WHERE message_time >= '2025-06-01'
            {page_clause}
        )
        SELECT
            shift,
            COUNT(*) as total_messages,
            COUNT(*) FILTER (WHERE is_from_page = false) as received,
            COUNT(*) FILTER (WHERE is_from_page = true) as sent
        FROM shifts
        GROUP BY shift
        ORDER BY
            CASE shift
                WHEN 'Morning (6am-2pm)' THEN 1
                WHEN 'Mid (2pm-10pm)' THEN 2
                ELSE 3
            END
    """, conn)

    comments_df = pd.read_sql(f"""
        WITH shifts AS (
            SELECT
                CASE
                    WHEN EXTRACT(HOUR FROM (comment_time AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')) BETWEEN 6 AND 13 THEN 'Morning (6am-2pm)'
                    WHEN EXTRACT(HOUR FROM (comment_time AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')) BETWEEN 14 AND 21 THEN 'Mid (2pm-10pm)'
                    ELSE 'Evening (10pm-6am)'
                END as shift
            FROM comments
            WHERE comment_time >= '2025-06-01'
            {page_clause}
        )
        SELECT
            shift,
            COUNT(*) as total_comments
        FROM shifts
        GROUP BY shift
        ORDER BY
            CASE shift
                WHEN 'Morning (6am-2pm)' THEN 1
                WHEN 'Mid (2pm-10pm)' THEN 2
                ELSE 3
            END
    """, conn)

    return messages_df, comments_df


@st.cache_data(ttl=60)
def get_page_rankings():
    """Get page rankings by messages and comments"""
    conn = get_connection()

    messages_df = pd.read_sql("""
        SELECT
            p.page_name,
            COUNT(*) as total_messages,
            COUNT(*) FILTER (WHERE m.is_from_page = false) as received,
            COUNT(*) FILTER (WHERE m.is_from_page = true) as sent,
            ROUND(100.0 * COUNT(*) FILTER (WHERE m.is_from_page = true) /
                  NULLIF(COUNT(*) FILTER (WHERE m.is_from_page = false), 0), 1) as response_rate
        FROM pages p
        LEFT JOIN messages m ON p.page_id = m.page_id AND m.message_time >= '2025-06-01'
        GROUP BY p.page_name
        HAVING COUNT(*) > 0
        ORDER BY total_messages DESC
    """, conn)

    comments_df = pd.read_sql("""
        SELECT
            p.page_name,
            COUNT(*) as total_comments,
            COUNT(DISTINCT c.author_id) as unique_commenters
        FROM pages p
        LEFT JOIN comments c ON p.page_id = c.page_id AND c.comment_time >= '2025-06-01'
        GROUP BY p.page_name
        HAVING COUNT(*) > 0
        ORDER BY total_comments DESC
    """, conn)

    sessions_df = pd.read_sql("""
        SELECT
            p.page_name,
            COUNT(*) as total_sessions,
            ROUND(AVG(s.duration_seconds)::numeric, 0) as avg_duration,
            ROUND(AVG(s.avg_response_time_seconds)::numeric, 0) as avg_response_time
        FROM pages p
        LEFT JOIN sessions s ON p.page_id = s.page_id AND s.session_start >= '2025-06-01'
        GROUP BY p.page_name
        ORDER BY total_sessions DESC
    """, conn)

    return messages_df, comments_df, sessions_df


# ============================================
# SIDEBAR
# ============================================
with st.sidebar:
    st.title("📊 Chat Analytics")
    st.markdown("---")

    # Page filter
    pages_df = get_pages()
    page_options = ["All Pages"] + pages_df["page_name"].tolist()
    selected_page = st.selectbox("🔍 Filter by Page", page_options)

    st.markdown("---")

    # View selector
    view = st.radio(
        "📈 Select View",
        ["Overview", "Daily", "Weekly", "Monthly", "Shifts", "Pages"],
        index=0
    )

    st.markdown("---")
    st.caption("Data from June 2025 onwards")
    st.caption("All times in Philippine Time (UTC+8)")


# ============================================
# MAIN CONTENT
# ============================================

if view == "Overview":
    st.title("📊 Overview Dashboard")

    if selected_page != "All Pages":
        st.info(f"Showing data for: **{selected_page}**")

    stats = get_overview_stats(selected_page)

    # Metrics row
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("💬 Total Messages", f"{stats['messages']['total_messages']:,}")
    with col2:
        st.metric("📥 Received", f"{stats['messages']['received']:,}")
    with col3:
        st.metric("📤 Sent", f"{stats['messages']['sent']:,}")
    with col4:
        response_rate = 0
        if stats['messages']['received'] > 0:
            response_rate = round(100 * stats['messages']['sent'] / stats['messages']['received'], 1)
        st.metric("📊 Response Rate", f"{response_rate}%")

    st.markdown("---")

    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric("💭 Total Comments", f"{stats['comments']['total_comments']:,}")
    with col2:
        st.metric("👥 Unique Commenters", f"{stats['comments']['unique_commenters']:,}")
    with col3:
        st.metric("🔄 Total Sessions", f"{stats['sessions']['total_sessions']:,}")

    # Charts
    st.markdown("---")
    st.subheader("📈 Last 30 Days Trend")

    messages_df, comments_df = get_daily_data(selected_page, 30)

    if not messages_df.empty:
        fig = px.line(messages_df, x='date', y=['received', 'sent'],
                      title='Messages Trend',
                      labels={'value': 'Count', 'date': 'Date'},
                      color_discrete_map={'received': '#3b82f6', 'sent': '#10b981'})
        st.plotly_chart(fig, use_container_width=True)

    if not comments_df.empty:
        fig = px.bar(comments_df, x='date', y='total_comments',
                     title='Comments Trend',
                     color_discrete_sequence=['#8b5cf6'])
        st.plotly_chart(fig, use_container_width=True)


elif view == "Daily":
    st.title("📅 Daily Analysis")

    if selected_page != "All Pages":
        st.info(f"Showing data for: **{selected_page}**")

    days = st.slider("Number of days", 7, 90, 30)
    messages_df, comments_df = get_daily_data(selected_page, days)

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("💬 Messages")
        if not messages_df.empty:
            st.dataframe(messages_df.sort_values('date', ascending=False), use_container_width=True)

    with col2:
        st.subheader("💭 Comments")
        if not comments_df.empty:
            st.dataframe(comments_df.sort_values('date', ascending=False), use_container_width=True)

    # Charts
    if not messages_df.empty:
        fig = px.area(messages_df, x='date', y='total_messages',
                      title='Daily Messages',
                      color_discrete_sequence=['#3b82f6'])
        st.plotly_chart(fig, use_container_width=True)

    if not comments_df.empty:
        fig = px.area(comments_df, x='date', y='total_comments',
                      title='Daily Comments',
                      color_discrete_sequence=['#8b5cf6'])
        st.plotly_chart(fig, use_container_width=True)


elif view == "Weekly":
    st.title("📆 Weekly Analysis")

    if selected_page != "All Pages":
        st.info(f"Showing data for: **{selected_page}**")

    messages_df, comments_df = get_weekly_data(selected_page, 12)

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("💬 Weekly Messages")
        if not messages_df.empty:
            messages_df['response_rate'] = (100 * messages_df['sent'] / messages_df['received'].replace(0, 1)).round(1)
            st.dataframe(messages_df, use_container_width=True)

    with col2:
        st.subheader("💭 Weekly Comments")
        if not comments_df.empty:
            st.dataframe(comments_df, use_container_width=True)

    # Charts
    if not messages_df.empty:
        fig = px.bar(messages_df.sort_values('week_start'), x='week_start', y=['received', 'sent'],
                     title='Weekly Messages',
                     barmode='group',
                     color_discrete_map={'received': '#3b82f6', 'sent': '#10b981'})
        st.plotly_chart(fig, use_container_width=True)


elif view == "Monthly":
    st.title("📊 Monthly Analysis")

    if selected_page != "All Pages":
        st.info(f"Showing data for: **{selected_page}**")

    messages_df, comments_df = get_monthly_data(selected_page)

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("💬 Monthly Messages")
        if not messages_df.empty:
            messages_df['response_rate'] = (100 * messages_df['sent'] / messages_df['received'].replace(0, 1)).round(1)
            st.dataframe(messages_df[['month_name', 'total_messages', 'received', 'sent', 'response_rate']],
                        use_container_width=True)

    with col2:
        st.subheader("💭 Monthly Comments")
        if not comments_df.empty:
            st.dataframe(comments_df[['month_name', 'total_comments', 'unique_commenters']],
                        use_container_width=True)

    # Charts
    if not messages_df.empty:
        fig = px.bar(messages_df.sort_values('month'), x='month_name', y='total_messages',
                     title='Monthly Messages',
                     color_discrete_sequence=['#3b82f6'])
        st.plotly_chart(fig, use_container_width=True)

    if not comments_df.empty:
        fig = px.bar(comments_df.sort_values('month'), x='month_name', y='total_comments',
                     title='Monthly Comments',
                     color_discrete_sequence=['#8b5cf6'])
        st.plotly_chart(fig, use_container_width=True)


elif view == "Shifts":
    st.title("⏰ Shift Analysis")

    if selected_page != "All Pages":
        st.info(f"Showing data for: **{selected_page}**")

    messages_df, comments_df = get_shift_data(selected_page)

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("💬 Messages by Shift")
        if not messages_df.empty:
            messages_df['response_rate'] = (100 * messages_df['sent'] / messages_df['received'].replace(0, 1)).round(1)
            st.dataframe(messages_df, use_container_width=True)

            fig = px.pie(messages_df, values='total_messages', names='shift',
                        title='Messages Distribution',
                        color_discrete_sequence=['#fbbf24', '#3b82f6', '#6366f1'])
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("💭 Comments by Shift")
        if not comments_df.empty:
            st.dataframe(comments_df, use_container_width=True)

            fig = px.pie(comments_df, values='total_comments', names='shift',
                        title='Comments Distribution',
                        color_discrete_sequence=['#fbbf24', '#8b5cf6', '#6366f1'])
            st.plotly_chart(fig, use_container_width=True)


elif view == "Pages":
    st.title("📄 Page Rankings")

    messages_df, comments_df, sessions_df = get_page_rankings()

    tab1, tab2, tab3 = st.tabs(["💬 Messages", "💭 Comments", "🔄 Sessions"])

    with tab1:
        st.subheader("Messages by Page")
        if not messages_df.empty:
            st.dataframe(messages_df, use_container_width=True)

            fig = px.bar(messages_df.head(10), x='page_name', y='total_messages',
                        title='Top 10 Pages by Messages',
                        color='response_rate',
                        color_continuous_scale='RdYlGn')
            st.plotly_chart(fig, use_container_width=True)

    with tab2:
        st.subheader("Comments by Page")
        if not comments_df.empty:
            st.dataframe(comments_df, use_container_width=True)

            fig = px.bar(comments_df.head(10), x='page_name', y='total_comments',
                        title='Top 10 Pages by Comments',
                        color_discrete_sequence=['#8b5cf6'])
            st.plotly_chart(fig, use_container_width=True)

    with tab3:
        st.subheader("Sessions by Page")
        if not sessions_df.empty:
            st.dataframe(sessions_df, use_container_width=True)

            fig = px.bar(sessions_df.head(10), x='page_name', y='total_sessions',
                        title='Top 10 Pages by Sessions',
                        color_discrete_sequence=['#10b981'])
            st.plotly_chart(fig, use_container_width=True)
