"""
Message Review - Agent Conversation Viewer
View and review agent message conversations with timestamps
"""

import streamlit as st
import pandas as pd
from datetime import date, timedelta, datetime
import pytz

# Import shared modules
from config import (
    CORE_PAGES, CORE_PAGES_SQL, TIMEZONE, CACHE_TTL, COLORS,
    LIVESTREAM_PAGES_SQL, SOCMED_PAGES_SQL
)
from db_utils import get_simple_connection as get_connection
from utils import format_rt

# Page config
st.set_page_config(
    page_title="Message Review",
    page_icon="💬",
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
def get_agent_conversations(agent_name, start_date, end_date, page_filter_sql):
    """Get conversations handled by an agent within date range"""
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        WITH agent_pages AS (
            SELECT DISTINCT apa.page_id, apa.shift
            FROM agents a
            JOIN agent_page_assignments apa ON a.id = apa.agent_id
            WHERE a.agent_name = %s AND apa.is_active = true
        )
        SELECT DISTINCT
            c.conversation_id,
            c.participant_name,
            p.page_name,
            c.updated_time,
            c.message_count,
            (SELECT COUNT(*) FROM messages m WHERE m.conversation_id = c.conversation_id) as actual_msgs
        FROM conversations c
        JOIN pages p ON c.page_id = p.page_id
        JOIN agent_pages ap ON c.page_id = ap.page_id
        WHERE c.updated_time::date BETWEEN %s AND %s
          AND p.page_name IN %s
        ORDER BY c.updated_time DESC
        LIMIT 100
    """, (agent_name, start_date, end_date, page_filter_sql))

    rows = cur.fetchall()
    cur.close()
    conn.close()

    return pd.DataFrame(rows, columns=[
        'conversation_id', 'participant_name', 'page_name',
        'updated_time', 'message_count', 'actual_msgs'
    ])


@st.cache_data(ttl=60)  # Short cache for real-time feel
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


@st.cache_data(ttl=CACHE_TTL["default"])
def get_agent_message_stats(agent_name, start_date, end_date):
    """Get message statistics for an agent"""
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT
            SUM(ads.messages_received) FILTER (WHERE ads.schedule_status = 'present') as total_recv,
            SUM(ads.messages_sent) FILTER (WHERE ads.schedule_status = 'present') as total_sent,
            AVG(ads.avg_response_time_seconds) FILTER (WHERE ads.avg_response_time_seconds > 10 AND ads.schedule_status = 'present') as avg_rt,
            COUNT(DISTINCT ads.date) FILTER (WHERE ads.schedule_status = 'present') as days_present
        FROM agents a
        JOIN agent_daily_stats ads ON a.id = ads.agent_id
        WHERE a.agent_name = %s
          AND ads.date BETWEEN %s AND %s
    """, (agent_name, start_date, end_date))

    row = cur.fetchone()
    cur.close()
    conn.close()

    return {
        'total_recv': row[0] or 0,
        'total_sent': row[1] or 0,
        'avg_rt': row[2],
        'days_present': row[3] or 0
    }


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


def display_message(row, is_page_reply):
    """Display a single message with styling"""
    if is_page_reply:
        # Page reply - right aligned, blue background
        col1, col2 = st.columns([1, 3])
        with col2:
            st.markdown(f"""
            <div style="background-color: #e3f2fd; padding: 10px; border-radius: 10px; margin: 5px 0;">
                <div style="font-size: 12px; color: #666;">
                    <b>{row['sender_name'] or 'Page'}</b>
                    <span style="float: right;">{format_message_time(row['message_time'])}</span>
                </div>
                <div style="margin-top: 5px;">{row['message_text'] or '[No text]'}</div>
                {f'<div style="font-size: 11px; color: #888; margin-top: 5px;">Response time: {format_rt(row["response_time_seconds"])}</div>' if row['response_time_seconds'] and row['response_time_seconds'] > 0 else ''}
            </div>
            """, unsafe_allow_html=True)
    else:
        # User message - left aligned, gray background
        col1, col2 = st.columns([3, 1])
        with col1:
            st.markdown(f"""
            <div style="background-color: #f5f5f5; padding: 10px; border-radius: 10px; margin: 5px 0;">
                <div style="font-size: 12px; color: #666;">
                    <b>{row['sender_name'] or 'User'}</b>
                    <span style="float: right;">{format_message_time(row['message_time'])}</span>
                </div>
                <div style="margin-top: 5px;">{row['message_text'] or '[No text]'}</div>
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
    st.title("Message Review")
    st.caption(f"Review agent conversations | Showing: {page_filter_name}")

st.markdown("---")

# Sidebar - Filters
with st.sidebar:
    st.header("Review Settings")

    # Agent selection
    agents = get_active_agents()
    agent_names = [a[0] for a in agents]

    if not agent_names:
        st.error("No active agents found")
        st.stop()

    selected_agent = st.selectbox("Select Agent", agent_names)

    st.markdown("---")

    # Date range
    today = date.today()
    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input("From", today - timedelta(days=7))
    with col2:
        end_date = st.date_input("To", today)

    st.markdown("---")

    # Agent stats summary
    if selected_agent:
        stats = get_agent_message_stats(selected_agent, start_date, end_date)
        st.subheader("Agent Summary")
        st.metric("Messages Received", f"{stats['total_recv']:,}")
        st.metric("Messages Sent", f"{stats['total_sent']:,}")
        st.metric("Avg Response Time", format_rt(stats['avg_rt']))
        st.metric("Days Present", stats['days_present'])

# Main content
if selected_agent:
    # Get conversations
    conversations = get_agent_conversations(selected_agent, start_date, end_date, page_filter_sql)

    if conversations.empty:
        st.info(f"No conversations found for {selected_agent} in the selected date range.")
    else:
        st.subheader(f"Conversations for {selected_agent}")
        st.caption(f"Found {len(conversations)} conversations (showing latest 100)")

        # Conversation list
        col1, col2 = st.columns([1, 2])

        with col1:
            st.markdown("### Conversations")

            # Format for display
            conv_display = conversations.copy()
            conv_display['updated_time'] = pd.to_datetime(conv_display['updated_time']).dt.strftime('%Y-%m-%d %H:%M')
            conv_display['participant_name'] = conv_display['participant_name'].fillna('Unknown User')

            # Create clickable list
            for idx, row in conv_display.iterrows():
                with st.expander(f"**{row['participant_name'][:30]}** - {row['page_name']}", expanded=False):
                    st.caption(f"Last updated: {row['updated_time']}")
                    st.caption(f"Messages: {row['actual_msgs']}")
                    if st.button("View Messages", key=f"view_{row['conversation_id']}"):
                        st.session_state['selected_conversation'] = row['conversation_id']
                        st.session_state['selected_participant'] = row['participant_name']

        with col2:
            st.markdown("### Message Thread")

            selected_conv = st.session_state.get('selected_conversation')
            selected_participant = st.session_state.get('selected_participant', 'Unknown')

            if selected_conv:
                st.caption(f"Conversation with: **{selected_participant}**")

                messages = get_conversation_messages(selected_conv)

                if messages.empty:
                    st.info("No messages found in this conversation")
                else:
                    # Calculate response time stats
                    page_msgs = messages[messages['is_from_page'] == True]
                    if not page_msgs.empty:
                        valid_rt = page_msgs[page_msgs['response_time_seconds'] > 0]['response_time_seconds']
                        if not valid_rt.empty:
                            avg_rt = valid_rt.mean()
                            st.info(f"Average response time in this conversation: **{format_rt(avg_rt)}**")

                    # Display messages
                    st.markdown("---")
                    for idx, row in messages.iterrows():
                        display_message(row, row['is_from_page'])
            else:
                st.info("Select a conversation from the left to view messages")

# Footer
st.markdown("---")
st.caption("All times in Philippine Time (UTC+8) | Select a conversation to review messages")
