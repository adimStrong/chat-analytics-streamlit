"""
Spill Review - Conversation Resolution Tracker
Review conversations with and without closing messages (spill keywords)
"""

import streamlit as st
import pandas as pd
from datetime import date, timedelta, datetime
import pytz

# Import shared modules
from config import (
    CORE_PAGES, CORE_PAGES_SQL, TIMEZONE, CACHE_TTL, COLORS,
    LIVESTREAM_PAGES_SQL, SOCMED_PAGES_SQL,
    SPILL_KEYWORDS, SPILL_START_DATE
)
from db_utils import get_simple_connection as get_connection
from utils import format_rt


def build_spill_sql_conditions():
    """Build SQL OR conditions for spill keyword detection"""
    conditions = []
    for keyword in SPILL_KEYWORDS:
        escaped = keyword.replace("'", "''").lower()
        conditions.append(f"LOWER(message_text) LIKE '%%{escaped}%%'")
    return " OR ".join(conditions)


# Page config
st.set_page_config(
    page_title="Spill Review",
    page_icon="🎯",
    layout="wide"
)

# Timezone
PHT = pytz.timezone('Asia/Manila')


# ============================================
# DATA FUNCTIONS
# ============================================

@st.cache_data(ttl=CACHE_TTL["default"])
def get_active_agents():
    """Get list of active agents"""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT DISTINCT a.agent_name, a.id
        FROM agents a
        WHERE a.is_active = true
        ORDER BY a.agent_name
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


@st.cache_data(ttl=CACHE_TTL["default"])
def get_spill_conversations(agent_name, start_date, end_date, page_filter_sql, with_spill=True):
    """Get conversations with or without spill keywords"""
    conn = get_connection()
    cur = conn.cursor()

    spill_conditions = build_spill_sql_conditions()

    query = f"""
        WITH agent_pages AS (
            SELECT DISTINCT apa.page_id
            FROM agents a
            JOIN agent_page_assignments apa ON a.id = apa.agent_id
            WHERE a.agent_name = %s AND apa.is_active = true
        ),
        agent_convos AS (
            SELECT DISTINCT
                c.conversation_id,
                c.participant_name,
                p.page_name,
                c.updated_time,
                c.message_count
            FROM conversations c
            JOIN agent_pages ap ON c.page_id = ap.page_id
            JOIN pages p ON c.page_id = p.page_id
            WHERE c.updated_time::date BETWEEN %s AND %s
              AND p.page_name IN %s
        ),
        convos_with_spill AS (
            SELECT DISTINCT ac.conversation_id
            FROM agent_convos ac
            JOIN messages m ON ac.conversation_id = m.conversation_id
            WHERE m.is_from_page = true
              AND ({spill_conditions})
        )
        SELECT
            ac.conversation_id,
            ac.participant_name,
            ac.page_name,
            ac.updated_time,
            ac.message_count,
            (SELECT COUNT(*) FROM messages m WHERE m.conversation_id = ac.conversation_id) as actual_msgs,
            CASE WHEN cws.conversation_id IS NOT NULL THEN true ELSE false END as has_spill
        FROM agent_convos ac
        LEFT JOIN convos_with_spill cws ON ac.conversation_id = cws.conversation_id
        WHERE {'cws.conversation_id IS NOT NULL' if with_spill else 'cws.conversation_id IS NULL'}
          AND (SELECT COUNT(*) FROM messages m WHERE m.conversation_id = ac.conversation_id) > 0
        ORDER BY ac.updated_time DESC
        LIMIT 100
    """

    cur.execute(query, (agent_name, start_date, end_date, page_filter_sql))
    rows = cur.fetchall()
    cur.close()
    conn.close()

    return pd.DataFrame(rows, columns=[
        'conversation_id', 'participant_name', 'page_name',
        'updated_time', 'message_count', 'actual_msgs', 'has_spill'
    ])


@st.cache_data(ttl=CACHE_TTL["default"])
def get_spill_stats(agent_name, start_date, end_date, page_filter_sql):
    """Get spill statistics for an agent"""
    conn = get_connection()
    cur = conn.cursor()

    spill_conditions = build_spill_sql_conditions()

    query = f"""
        WITH agent_pages AS (
            SELECT DISTINCT apa.page_id
            FROM agents a
            JOIN agent_page_assignments apa ON a.id = apa.agent_id
            WHERE a.agent_name = %s AND apa.is_active = true
        ),
        agent_convos AS (
            SELECT DISTINCT c.conversation_id
            FROM conversations c
            JOIN agent_pages ap ON c.page_id = ap.page_id
            JOIN pages p ON c.page_id = p.page_id
            WHERE c.updated_time::date BETWEEN %s AND %s
              AND p.page_name IN %s
              AND EXISTS (SELECT 1 FROM messages m WHERE m.conversation_id = c.conversation_id)
        ),
        resolved_convos AS (
            SELECT DISTINCT ac.conversation_id
            FROM agent_convos ac
            JOIN messages m ON ac.conversation_id = m.conversation_id
            WHERE m.is_from_page = true
              AND ({spill_conditions})
        )
        SELECT
            (SELECT COUNT(*) FROM agent_convos) as total_convos,
            (SELECT COUNT(*) FROM resolved_convos) as resolved_convos
    """

    cur.execute(query, (agent_name, start_date, end_date, page_filter_sql))
    row = cur.fetchone()
    cur.close()
    conn.close()

    total = row[0] or 0
    resolved = row[1] or 0
    return {
        'total': total,
        'resolved': resolved,
        'unresolved': total - resolved,
        'rate': (resolved / total * 100) if total > 0 else 0
    }


@st.cache_data(ttl=CACHE_TTL["default"])
def get_all_agents_spill_stats(start_date, end_date, page_filter_sql):
    """Get spill statistics for all agents"""
    conn = get_connection()
    cur = conn.cursor()

    spill_conditions = build_spill_sql_conditions()

    query = f"""
        WITH agent_convos AS (
            SELECT
                a.agent_name,
                c.conversation_id
            FROM agents a
            JOIN agent_page_assignments apa ON a.id = apa.agent_id AND apa.is_active = true
            JOIN pages p ON apa.page_id = p.page_id
            JOIN conversations c ON c.page_id = apa.page_id
            WHERE a.is_active = true
              AND p.page_name IN %s
              AND c.updated_time::date BETWEEN %s AND %s
              AND EXISTS (SELECT 1 FROM messages m WHERE m.conversation_id = c.conversation_id)
        ),
        resolved_convos AS (
            SELECT DISTINCT ac.agent_name, ac.conversation_id
            FROM agent_convos ac
            JOIN messages m ON ac.conversation_id = m.conversation_id
            WHERE m.is_from_page = true
              AND ({spill_conditions})
        )
        SELECT
            ac.agent_name,
            COUNT(DISTINCT ac.conversation_id) as total_convos,
            COUNT(DISTINCT rc.conversation_id) as resolved_convos
        FROM agent_convos ac
        LEFT JOIN resolved_convos rc ON ac.agent_name = rc.agent_name
                                     AND ac.conversation_id = rc.conversation_id
        GROUP BY ac.agent_name
        HAVING COUNT(DISTINCT ac.conversation_id) > 0
        ORDER BY ac.agent_name
    """

    cur.execute(query, (page_filter_sql, start_date, end_date))
    rows = cur.fetchall()
    cur.close()
    conn.close()

    return pd.DataFrame(rows, columns=['agent_name', 'total_convos', 'resolved_convos'])


@st.cache_data(ttl=60)
def get_conversation_messages(conversation_id):
    """Get all messages in a conversation"""
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT
            m.message_id,
            m.sender_name,
            m.message_text,
            m.message_time,
            m.is_from_page,
            m.response_time_seconds,
            p.page_name
        FROM messages m
        JOIN pages p ON m.page_id = p.page_id
        WHERE m.conversation_id = %s
        ORDER BY m.message_time ASC
    """, (conversation_id,))

    rows = cur.fetchall()
    cur.close()
    conn.close()

    return pd.DataFrame(rows, columns=[
        'message_id', 'sender_name', 'message_text',
        'message_time', 'is_from_page', 'response_time_seconds', 'page_name'
    ])


# ============================================
# DISPLAY FUNCTIONS
# ============================================

def format_message_time(msg_time):
    """Format message time to Philippine timezone"""
    if msg_time is None:
        return "N/A"
    if isinstance(msg_time, str):
        msg_time = datetime.fromisoformat(msg_time.replace('Z', '+00:00'))
    if msg_time.tzinfo is None:
        msg_time = pytz.utc.localize(msg_time)
    pht_time = msg_time.astimezone(PHT)
    return pht_time.strftime('%Y-%m-%d %H:%M:%S')


def highlight_spill_keywords(text):
    """Highlight spill keywords in message text"""
    if not text:
        return text

    highlighted = text
    for keyword in SPILL_KEYWORDS:
        # Case-insensitive replacement with highlight
        import re
        pattern = re.compile(re.escape(keyword), re.IGNORECASE)
        highlighted = pattern.sub(f'<mark style="background-color: #90EE90;">{keyword}</mark>', highlighted)

    return highlighted


def display_message(row, is_page_reply):
    """Display a single message with styling"""
    message_text = row['message_text'] or '[No text]'

    # Highlight spill keywords in page replies
    if is_page_reply:
        message_text = highlight_spill_keywords(message_text)

    if is_page_reply:
        col1, col2 = st.columns([1, 3])
        with col2:
            st.markdown(f"""
            <div style="background-color: #e3f2fd; padding: 10px; border-radius: 10px; margin: 5px 0;">
                <div style="font-size: 12px; color: #666;">
                    <b>{row['sender_name'] or 'Page'}</b>
                    <span style="float: right;">{format_message_time(row['message_time'])}</span>
                </div>
                <div style="margin-top: 5px;">{message_text}</div>
                {f'<div style="font-size: 11px; color: #888; margin-top: 5px;">Response time: {format_rt(row["response_time_seconds"])}</div>' if row['response_time_seconds'] and row['response_time_seconds'] > 0 else ''}
            </div>
            """, unsafe_allow_html=True)
    else:
        col1, col2 = st.columns([3, 1])
        with col1:
            st.markdown(f"""
            <div style="background-color: #f5f5f5; padding: 10px; border-radius: 10px; margin: 5px 0;">
                <div style="font-size: 12px; color: #666;">
                    <b>{row['sender_name'] or 'User'}</b>
                    <span style="float: right;">{format_message_time(row['message_time'])}</span>
                </div>
                <div style="margin-top: 5px;">{message_text}</div>
            </div>
            """, unsafe_allow_html=True)


# ============================================
# MAIN APP
# ============================================

# Get page filter from session state
page_filter_sql = st.session_state.get('page_filter_sql', CORE_PAGES_SQL)
page_filter_name = st.session_state.get('page_filter_name', 'All Pages')

# Logo and Title
col_logo, col_title = st.columns([0.08, 0.92])
with col_logo:
    st.image("Juan365.jpg", width=60)
with col_title:
    st.title("🎯 Spill Review")
    st.caption(f"Review conversation resolutions | Showing: {page_filter_name}")

st.markdown("---")

# Check spill tracking period
spill_start = datetime.strptime(SPILL_START_DATE, "%Y-%m-%d").date()
today = date.today()

# Sidebar - Filters
with st.sidebar:
    st.header("Filter Settings")

    # Date range
    st.subheader("Date Range")
    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input("From", max(spill_start, today - timedelta(days=7)))
    with col2:
        end_date = st.date_input("To", today)

    if start_date < spill_start:
        st.warning(f"Spill tracking starts {SPILL_START_DATE}")
        start_date = spill_start

    st.markdown("---")

    # Agent selection
    agents = get_active_agents()
    agent_names = ["All Agents"] + [a[0] for a in agents]
    selected_agent = st.selectbox("Select Agent", agent_names)

    st.markdown("---")

    # Spill keywords reference
    with st.expander("📋 Spill Keywords"):
        for keyword in SPILL_KEYWORDS:
            st.caption(f"• {keyword}")

# Main content
if start_date > end_date:
    st.error("Start date must be before end date")
    st.stop()

# Show all agents summary if "All Agents" selected
if selected_agent == "All Agents":
    st.subheader("All Agents Resolution Summary")

    all_stats = get_all_agents_spill_stats(start_date, end_date, page_filter_sql)

    if all_stats.empty:
        st.info("No conversations found in the selected date range")
    else:
        # Calculate rates
        all_stats['unresolved'] = all_stats['total_convos'] - all_stats['resolved_convos']
        all_stats['resolution_rate'] = (all_stats['resolved_convos'] / all_stats['total_convos'] * 100).round(1)

        # Overall stats
        total_all = all_stats['total_convos'].sum()
        resolved_all = all_stats['resolved_convos'].sum()
        unresolved_all = total_all - resolved_all
        rate_all = (resolved_all / total_all * 100) if total_all > 0 else 0

        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Conversations", f"{total_all:,}")
        with col2:
            st.metric("✅ Resolved", f"{resolved_all:,}")
        with col3:
            st.metric("❌ Unresolved", f"{unresolved_all:,}")
        with col4:
            st.metric("Resolution Rate", f"{rate_all:.1f}%")

        st.markdown("---")

        # Agent breakdown table
        st.subheader("Agent Breakdown")

        display_df = all_stats[['agent_name', 'total_convos', 'resolved_convos', 'unresolved', 'resolution_rate']].copy()
        display_df.columns = ['Agent', 'Total', 'Resolved', 'Unresolved', 'Rate %']
        display_df = display_df.sort_values('Rate %', ascending=False)

        st.dataframe(
            display_df,
            use_container_width=True,
            hide_index=True,
            column_config={
                'Agent': st.column_config.TextColumn('Agent', width='medium'),
                'Total': st.column_config.NumberColumn('Total', format='%d'),
                'Resolved': st.column_config.NumberColumn('✅ Resolved', format='%d'),
                'Unresolved': st.column_config.NumberColumn('❌ Unresolved', format='%d'),
                'Rate %': st.column_config.ProgressColumn('Resolution Rate', format='%.1f%%', min_value=0, max_value=100)
            }
        )

else:
    # Single agent view
    spill_stats = get_spill_stats(selected_agent, start_date, end_date, page_filter_sql)

    # Display stats
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Conversations", f"{spill_stats['total']:,}")
    with col2:
        st.metric("✅ With Spill", f"{spill_stats['resolved']:,}")
    with col3:
        st.metric("❌ Without Spill", f"{spill_stats['unresolved']:,}")
    with col4:
        st.metric("Resolution Rate", f"{spill_stats['rate']:.1f}%")

    st.markdown("---")

    # Tabs for with/without spill
    spill_tab1, spill_tab2 = st.tabs(["❌ Without Spill (Needs Review)", "✅ With Spill (Resolved)"])

    with spill_tab1:
        st.subheader("Conversations Without Closing Message")
        st.caption("These conversations may need follow-up - no spill keywords detected in page replies")

        convos_without = get_spill_conversations(selected_agent, start_date, end_date, page_filter_sql, with_spill=False)

        if convos_without.empty:
            st.success("All conversations have proper closing messages!")
        else:
            st.warning(f"Found {len(convos_without)} conversations without closing message")

            # Two column layout
            col_list, col_thread = st.columns([1, 2])

            with col_list:
                st.markdown("### Conversations")
                for idx, row in convos_without.iterrows():
                    participant = row['participant_name'] or 'Unknown User'
                    updated = pd.to_datetime(row['updated_time']).strftime('%m-%d %H:%M') if row['updated_time'] else 'N/A'

                    with st.expander(f"❌ {participant[:25]}"):
                        st.caption(f"Page: {row['page_name']}")
                        st.caption(f"Updated: {updated}")
                        st.caption(f"Messages: {row['actual_msgs']}")
                        if st.button("View", key=f"no_spill_{row['conversation_id']}"):
                            st.session_state['spill_selected_conv'] = row['conversation_id']
                            st.session_state['spill_selected_name'] = participant
                            st.rerun()

            with col_thread:
                st.markdown("### Message Thread")
                selected_conv = st.session_state.get('spill_selected_conv')
                selected_name = st.session_state.get('spill_selected_name', 'Unknown')

                if selected_conv:
                    st.caption(f"Conversation with: **{selected_name}**")
                    messages = get_conversation_messages(selected_conv)

                    if messages.empty:
                        st.info("No messages found")
                    else:
                        st.markdown("---")
                        for idx, msg_row in messages.iterrows():
                            display_message(msg_row, msg_row['is_from_page'])
                else:
                    st.info("Select a conversation to view messages")

    with spill_tab2:
        st.subheader("Conversations With Closing Message")
        st.caption("These conversations have proper closing messages with spill keywords")

        convos_with = get_spill_conversations(selected_agent, start_date, end_date, page_filter_sql, with_spill=True)

        if convos_with.empty:
            st.info("No resolved conversations found in this period")
        else:
            st.success(f"Found {len(convos_with)} resolved conversations")

            # Two column layout
            col_list, col_thread = st.columns([1, 2])

            with col_list:
                st.markdown("### Conversations")
                for idx, row in convos_with.iterrows():
                    participant = row['participant_name'] or 'Unknown User'
                    updated = pd.to_datetime(row['updated_time']).strftime('%m-%d %H:%M') if row['updated_time'] else 'N/A'

                    with st.expander(f"✅ {participant[:25]}"):
                        st.caption(f"Page: {row['page_name']}")
                        st.caption(f"Updated: {updated}")
                        st.caption(f"Messages: {row['actual_msgs']}")
                        if st.button("View", key=f"with_spill_{row['conversation_id']}"):
                            st.session_state['spill_selected_conv'] = row['conversation_id']
                            st.session_state['spill_selected_name'] = participant
                            st.rerun()

            with col_thread:
                st.markdown("### Message Thread")
                selected_conv = st.session_state.get('spill_selected_conv')
                selected_name = st.session_state.get('spill_selected_name', 'Unknown')

                if selected_conv:
                    st.caption(f"Conversation with: **{selected_name}**")
                    messages = get_conversation_messages(selected_conv)

                    if messages.empty:
                        st.info("No messages found")
                    else:
                        st.markdown("---")
                        for idx, msg_row in messages.iterrows():
                            display_message(msg_row, msg_row['is_from_page'])
                else:
                    st.info("Select a conversation to view messages")

# Footer
st.markdown("---")
st.caption(f"Spill tracking started: {SPILL_START_DATE} | All times in Philippine Time (UTC+8)")
