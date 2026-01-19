"""
Spiel Tracker Page
Tracks agent opening/closing spiels usage and attribution
"""

import streamlit as st
import pandas as pd
from datetime import date, timedelta
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from spiel_matcher import (
    AGENT_SPIELS, detect_spiel_owner, normalize_agent_name,
    get_supported_agents, clean_text, get_similarity
)
from config import SPIELS_START_DATE, CORE_PAGES

st.set_page_config(
    page_title="Spiel Tracker",
    page_icon="📊",
    layout="wide"
)


def get_meta_inbox_url(page_id: str, conversation_id: str) -> str:
    """Generate Meta Business Suite inbox URL for a conversation."""
    return f"https://business.facebook.com/latest/inbox/all?asset_id={page_id}&thread_id={conversation_id}"


def get_db_connection():
    """Get database connection."""
    import psycopg2
    try:
        if hasattr(st, 'secrets') and 'DATABASE_URL' in st.secrets:
            return psycopg2.connect(st.secrets['DATABASE_URL'])
    except Exception:
        pass

    from dotenv import load_dotenv
    load_dotenv()
    db_url = os.getenv('DATABASE_URL')
    if db_url:
        return psycopg2.connect(db_url)
    raise ValueError("DATABASE_URL not found")


@st.cache_data(ttl=60)
def get_spiel_stats_from_db(stat_date):
    """Get spiel stats from agent_daily_stats table for a single date."""
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT a.agent_name,
               COALESCE(ads.opening_spiels_count, 0) as opening,
               COALESCE(ads.closing_spiels_count, 0) as closing,
               ads.schedule_status
        FROM agents a
        LEFT JOIN agent_daily_stats ads ON a.id = ads.agent_id AND ads.date = %s
        WHERE a.is_active = true
        ORDER BY a.agent_name
    """, (stat_date,))

    data = cur.fetchall()
    cur.close()
    conn.close()

    return pd.DataFrame(data, columns=['Agent', 'Opening', 'Closing', 'Status'])


@st.cache_data(ttl=60)
def get_spiel_stats_date_range(start_date, end_date):
    """Get spiel stats from agent_daily_stats table for a date range."""
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT a.agent_name,
               COALESCE(SUM(ads.opening_spiels_count), 0) as opening,
               COALESCE(SUM(ads.closing_spiels_count), 0) as closing,
               COUNT(CASE WHEN ads.schedule_status = 'present' THEN 1 END) as days_present
        FROM agents a
        LEFT JOIN agent_daily_stats ads ON a.id = ads.agent_id
            AND ads.date BETWEEN %s AND %s
        WHERE a.is_active = true
        GROUP BY a.agent_name
        ORDER BY a.agent_name
    """, (start_date, end_date))

    data = cur.fetchall()
    cur.close()
    conn.close()

    return pd.DataFrame(data, columns=['Agent', 'Opening', 'Closing', 'Days Present'])


@st.cache_data(ttl=60)
def get_conversation_spiel_review(start_date, end_date=None):
    """
    Get spiels per conversation with proper flow tracking.
    Closing only counts if there was an opening first in the conversation.
    """
    if end_date is None:
        end_date = start_date

    conn = get_db_connection()
    cur = conn.cursor()

    # Get all outgoing messages grouped by conversation with timestamps
    cur.execute("""
        SELECT m.conversation_id, m.message_text, p.page_name,
               m.message_time AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila' as msg_time
        FROM messages m
        JOIN pages p ON m.page_id = p.page_id
        WHERE m.is_from_page = true
          AND (m.message_time AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')::date BETWEEN %s AND %s
          AND p.page_name IN %s
          AND (
              LOWER(m.message_text) LIKE '%%juanderful%%'
              OR LOWER(m.message_text) LIKE '%%juankada%%'
              OR LOWER(m.message_text) LIKE '%%juanted%%'
              OR LOWER(m.message_text) LIKE '%%maitutulong%%'
              OR LOWER(m.message_text) LIKE '%%game na game%%'
              OR LOWER(m.message_text) LIKE '%%nandito lang%%'
              OR LOWER(m.message_text) LIKE '%%salamat%%'
              OR LOWER(m.message_text) LIKE '%%thank you%%'
              OR LOWER(m.message_text) LIKE '%%good luck%%'
              OR LOWER(m.message_text) LIKE '%%play smart%%'
              OR LOWER(m.message_text) LIKE '%%stay in control%%'
              OR LOWER(m.message_text) LIKE '%%appreciate%%'
              OR LOWER(m.message_text) LIKE '%%reach out%%'
              OR LOWER(m.message_text) LIKE '%%feel free%%'
          )
        ORDER BY m.conversation_id, m.message_time
    """, (start_date, end_date, tuple(CORE_PAGES)))

    messages = cur.fetchall()
    cur.close()
    conn.close()

    # Track spiels per conversation
    conversations = {}  # conv_id -> {has_opening: bool, opening_owner: str, messages: []}

    for conv_id, msg_text, page_name, msg_time in messages:
        if not msg_text:
            continue

        if conv_id not in conversations:
            conversations[conv_id] = {
                'has_opening': False,
                'opening_owner': None,
                'opening_time': None,
                'has_closing': False,
                'closing_owner': None,
                'page': page_name,
                'messages': []
            }

        conv = conversations[conv_id]

        # Check for opening spiel
        opening_owner, opening_score = detect_spiel_owner(msg_text, "opening")
        if opening_owner and opening_score >= 0.70:
            # New opening resets the conversation tracking
            conv['has_opening'] = True
            conv['opening_owner'] = opening_owner
            conv['opening_time'] = msg_time
            conv['has_closing'] = False  # Reset closing when new opening
            conv['messages'].append({
                'time': msg_time,
                'text': msg_text,
                'type': 'Opening',
                'owner': opening_owner,
                'score': opening_score,
                'is_from_page': True
            })

        # Check for closing spiel
        closing_owner, closing_score = detect_spiel_owner(msg_text, "closing")
        if closing_owner and closing_score >= 0.70:
            conv['messages'].append({
                'time': msg_time,
                'text': msg_text,
                'type': 'Closing',
                'owner': closing_owner,
                'score': closing_score,
                'valid': conv['has_opening'],  # Only valid if opening exists
                'is_from_page': True
            })
            if conv['has_opening']:
                conv['has_closing'] = True
                conv['closing_owner'] = closing_owner

    return conversations


@st.cache_data(ttl=60)
def get_full_conversation_history(conv_ids: list, start_date, end_date=None):
    """
    Get ALL messages (including customer messages) for specified conversations.
    Includes page_id for Meta Business Suite links.
    """
    if end_date is None:
        end_date = start_date

    if not conv_ids:
        return {}

    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT m.conversation_id, m.message_text, m.is_from_page,
               m.message_time AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila' as msg_time,
               p.page_name, p.page_id
        FROM messages m
        JOIN pages p ON m.page_id = p.page_id
        WHERE m.conversation_id = ANY(%s)
          AND (m.message_time AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')::date BETWEEN %s AND %s
        ORDER BY m.conversation_id, m.message_time
    """, (conv_ids, start_date, end_date))

    messages = cur.fetchall()
    cur.close()
    conn.close()

    # Group by conversation
    conv_messages = {}
    for conv_id, msg_text, is_from_page, msg_time, page_name, page_id in messages:
        if conv_id not in conv_messages:
            conv_messages[conv_id] = {
                'page': page_name,
                'page_id': page_id,
                'messages': []
            }
        conv_messages[conv_id]['messages'].append({
            'time': msg_time,
            'text': msg_text or '',
            'is_from_page': is_from_page,
            'type': 'Agent' if is_from_page else 'Customer'
        })

    return conv_messages


@st.cache_data(ttl=60)
def get_all_conversations(start_date, end_date=None):
    """
    Get ALL conversations (with or without spiels) for the date range.
    Returns conversation IDs with basic info including page_id for Meta links.
    """
    if end_date is None:
        end_date = start_date

    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT DISTINCT m.conversation_id, p.page_name, p.page_id,
               MIN(m.message_time AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila') as first_msg,
               COUNT(*) as msg_count
        FROM messages m
        JOIN pages p ON m.page_id = p.page_id
        WHERE (m.message_time AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')::date BETWEEN %s AND %s
          AND p.page_name IN %s
        GROUP BY m.conversation_id, p.page_name, p.page_id
        ORDER BY first_msg DESC
        LIMIT 500
    """, (start_date, end_date, tuple(CORE_PAGES)))

    convs = cur.fetchall()
    cur.close()
    conn.close()

    return {
        row[0]: {
            'page': row[1],
            'page_id': row[2],
            'first_msg': row[3],
            'msg_count': row[4]
        }
        for row in convs
    }


@st.cache_data(ttl=60)
def get_message_counts(start_date, end_date=None):
    """
    Get total message counts for the date range.
    Returns total outgoing messages and spiel message count.
    """
    if end_date is None:
        end_date = start_date

    conn = get_db_connection()
    cur = conn.cursor()

    # Total outgoing messages
    cur.execute("""
        SELECT COUNT(*)
        FROM messages m
        JOIN pages p ON m.page_id = p.page_id
        WHERE m.is_from_page = true
          AND (m.message_time AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')::date BETWEEN %s AND %s
          AND p.page_name IN %s
    """, (start_date, end_date, tuple(CORE_PAGES)))
    total_outgoing = cur.fetchone()[0]

    # Messages with spiel keywords (potential spiels)
    cur.execute("""
        SELECT COUNT(*)
        FROM messages m
        JOIN pages p ON m.page_id = p.page_id
        WHERE m.is_from_page = true
          AND (m.message_time AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')::date BETWEEN %s AND %s
          AND p.page_name IN %s
          AND (
              LOWER(m.message_text) LIKE '%%juanderful%%'
              OR LOWER(m.message_text) LIKE '%%juankada%%'
              OR LOWER(m.message_text) LIKE '%%juanted%%'
              OR LOWER(m.message_text) LIKE '%%maitutulong%%'
              OR LOWER(m.message_text) LIKE '%%game na game%%'
              OR LOWER(m.message_text) LIKE '%%nandito lang%%'
              OR LOWER(m.message_text) LIKE '%%salamat%%'
              OR LOWER(m.message_text) LIKE '%%thank you%%'
              OR LOWER(m.message_text) LIKE '%%good luck%%'
              OR LOWER(m.message_text) LIKE '%%play smart%%'
              OR LOWER(m.message_text) LIKE '%%stay in control%%'
              OR LOWER(m.message_text) LIKE '%%appreciate%%'
              OR LOWER(m.message_text) LIKE '%%reach out%%'
              OR LOWER(m.message_text) LIKE '%%feel free%%'
          )
    """, (start_date, end_date, tuple(CORE_PAGES)))
    spiel_messages = cur.fetchone()[0]

    cur.close()
    conn.close()

    return {
        'total_outgoing': total_outgoing,
        'with_spiel': spiel_messages,
        'without_spiel': total_outgoing - spiel_messages
    }


@st.cache_data(ttl=60)
def get_live_spiel_detection(stat_date):
    """Detect spiels in real-time from messages table with attribution."""
    conn = get_db_connection()
    cur = conn.cursor()

    # Get all outgoing messages with key phrases
    cur.execute("""
        SELECT m.message_text, p.page_name
        FROM messages m
        JOIN pages p ON m.page_id = p.page_id
        WHERE m.is_from_page = true
          AND (m.message_time AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')::date = %s
          AND p.page_name IN %s
          AND (
              LOWER(m.message_text) LIKE '%%juanderful%%'
              OR LOWER(m.message_text) LIKE '%%juankada%%'
              OR LOWER(m.message_text) LIKE '%%juanted%%'
              OR LOWER(m.message_text) LIKE '%%maitutulong%%'
              OR LOWER(m.message_text) LIKE '%%game na game%%'
              OR LOWER(m.message_text) LIKE '%%nandito lang%%'
              OR LOWER(m.message_text) LIKE '%%salamat%%'
              OR LOWER(m.message_text) LIKE '%%thank you%%'
              OR LOWER(m.message_text) LIKE '%%good luck%%'
              OR LOWER(m.message_text) LIKE '%%play smart%%'
              OR LOWER(m.message_text) LIKE '%%stay in control%%'
              OR LOWER(m.message_text) LIKE '%%appreciate%%'
              OR LOWER(m.message_text) LIKE '%%reach out%%'
              OR LOWER(m.message_text) LIKE '%%feel free%%'
          )
    """, (stat_date, tuple(CORE_PAGES)))

    messages = cur.fetchall()
    cur.close()
    conn.close()

    # Detect spiel ownership
    results = []
    for msg_text, page_name in messages:
        if not msg_text:
            continue

        # Check opening
        opening_owner, opening_score = detect_spiel_owner(msg_text, "opening")
        if opening_owner:
            results.append({
                'message': msg_text[:100] + '...' if len(msg_text) > 100 else msg_text,
                'page': page_name,
                'type': 'Opening',
                'credited_to': opening_owner,
                'match_score': f"{opening_score:.1%}"
            })

        # Check closing
        closing_owner, closing_score = detect_spiel_owner(msg_text, "closing")
        if closing_owner:
            results.append({
                'message': msg_text[:100] + '...' if len(msg_text) > 100 else msg_text,
                'page': page_name,
                'type': 'Closing',
                'credited_to': closing_owner,
                'match_score': f"{closing_score:.1%}"
            })

    return pd.DataFrame(results)


@st.cache_data(ttl=300)
def get_spiel_trend(days=7):
    """Get spiel counts over time."""
    conn = get_db_connection()
    cur = conn.cursor()

    start_date = date.today() - timedelta(days=days)

    cur.execute("""
        SELECT ads.date,
               a.agent_name,
               COALESCE(ads.opening_spiels_count, 0) as opening,
               COALESCE(ads.closing_spiels_count, 0) as closing
        FROM agent_daily_stats ads
        JOIN agents a ON ads.agent_id = a.id
        WHERE ads.date >= %s
          AND a.is_active = true
        ORDER BY ads.date, a.agent_name
    """, (start_date,))

    data = cur.fetchall()
    cur.close()
    conn.close()

    return pd.DataFrame(data, columns=['Date', 'Agent', 'Opening', 'Closing'])


def main():
    st.title("📊 Spiel Tracker")
    st.markdown("Track agent opening and closing spiels. **Spiels are credited to the spiel OWNER**, not the sending agent.")

    spiels_start = date(2026, 1, 16)  # SPIELS_START_DATE

    # Date selection mode
    col1, col2, col3 = st.columns([1, 1, 2])
    with col1:
        date_mode = st.radio("Date Mode", ["Single Date", "Date Range"], horizontal=True)

    if date_mode == "Single Date":
        with col2:
            default_date = max(date.today() - timedelta(days=1), spiels_start)
            selected_date = st.date_input(
                "Select Date",
                value=default_date,
                min_value=spiels_start,
                max_value=date.today()
            )
        start_date = selected_date
        end_date = selected_date
        date_label = selected_date.strftime('%b %d, %Y')
    else:
        with col2:
            start_date = st.date_input(
                "Start Date",
                value=spiels_start,
                min_value=spiels_start,
                max_value=date.today()
            )
        with col3:
            end_date = st.date_input(
                "End Date",
                value=date.today(),
                min_value=start_date,
                max_value=date.today()
            )
        num_days = (end_date - start_date).days + 1
        date_label = f"{start_date.strftime('%b %d')} - {end_date.strftime('%b %d, %Y')} ({num_days} days)"

    st.divider()

    # Main metrics
    st.subheader(f"📅 Spiel Summary for {date_label}")

    # Get data based on mode
    if date_mode == "Single Date":
        db_stats = get_spiel_stats_from_db(selected_date)
        status_col = 'Status'
    else:
        db_stats = get_spiel_stats_date_range(start_date, end_date)
        status_col = 'Days Present'

    # Get conversation data for summary
    conversations_for_summary = get_conversation_spiel_review(start_date, end_date)
    all_convs_for_summary = get_all_conversations(start_date, end_date)
    total_all_convs_summary = len(all_convs_for_summary)
    convs_with_spiels_summary = len(conversations_for_summary)
    convs_without_spiels_summary = total_all_convs_summary - convs_with_spiels_summary
    complete_flow_summary = sum(1 for c in conversations_for_summary.values() if c['has_opening'] and c['has_closing'])

    # Get message counts
    msg_counts = get_message_counts(start_date, end_date)

    # Filter to agents with spiels configured
    supported_agents = get_supported_agents()
    spiel_agents = db_stats[db_stats['Agent'].apply(
        lambda x: normalize_agent_name(x) in supported_agents
    )].copy()

    if not spiel_agents.empty or total_all_convs_summary > 0:
        # Spiel counts row
        st.markdown("##### Spiel Counts")
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Opening", int(spiel_agents['Opening'].sum()) if not spiel_agents.empty else 0)
        with col2:
            st.metric("Total Closing", int(spiel_agents['Closing'].sum()) if not spiel_agents.empty else 0)
        with col3:
            total_spiels = int(spiel_agents['Opening'].sum() + spiel_agents['Closing'].sum()) if not spiel_agents.empty else 0
            st.metric("Total Spiels", total_spiels)
        with col4:
            agents_with_spiels = len(spiel_agents[(spiel_agents['Opening'] > 0) | (spiel_agents['Closing'] > 0)]) if not spiel_agents.empty else 0
            st.metric("Agents with Spiels", f"{agents_with_spiels}/{len(spiel_agents)}" if not spiel_agents.empty else "0/0")

        # Message counts row
        st.markdown("##### Message Counts (Outgoing)")
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Messages", msg_counts['total_outgoing'])
        with col2:
            st.metric("With Spiel Keywords", msg_counts['with_spiel'])
        with col3:
            st.metric("Without Spiel Keywords", msg_counts['without_spiel'])
        with col4:
            msg_spiel_rate = (msg_counts['with_spiel'] / msg_counts['total_outgoing'] * 100) if msg_counts['total_outgoing'] > 0 else 0
            st.metric("Spiel Msg Rate", f"{msg_spiel_rate:.1f}%")

        # Conversation counts row
        st.markdown("##### Conversation Counts")
        col1, col2, col3, col4, col5 = st.columns(5)
        with col1:
            st.metric("Total Conversations", total_all_convs_summary)
        with col2:
            st.metric("With Spiels", convs_with_spiels_summary)
        with col3:
            st.metric("Without Spiels", convs_without_spiels_summary)
        with col4:
            st.metric("Complete Flow", complete_flow_summary)
        with col5:
            spiel_rate = (convs_with_spiels_summary / total_all_convs_summary * 100) if total_all_convs_summary > 0 else 0
            st.metric("Spiel Rate", f"{spiel_rate:.1f}%")

        st.divider()

        # Agent breakdown table
        st.subheader("👤 Spiel Counts by Agent (Credited as Owner)")

        display_df = spiel_agents[['Agent', 'Opening', 'Closing', status_col]].copy()
        display_df['Total'] = display_df['Opening'] + display_df['Closing']
        display_df = display_df.sort_values('Total', ascending=False)

        # Add normalized name for reference
        display_df['Spiel Key'] = display_df['Agent'].apply(normalize_agent_name)

        # Column config based on mode
        if date_mode == "Single Date":
            col_config = {
                'Agent': st.column_config.TextColumn('Agent Name'),
                'Opening': st.column_config.NumberColumn('Opening', format="%d"),
                'Closing': st.column_config.NumberColumn('Closing', format="%d"),
                'Total': st.column_config.NumberColumn('Total', format="%d"),
                'Status': st.column_config.TextColumn('Schedule'),
                'Spiel Key': st.column_config.TextColumn('Spiel Config')
            }
        else:
            col_config = {
                'Agent': st.column_config.TextColumn('Agent Name'),
                'Opening': st.column_config.NumberColumn('Opening', format="%d"),
                'Closing': st.column_config.NumberColumn('Closing', format="%d"),
                'Total': st.column_config.NumberColumn('Total', format="%d"),
                'Days Present': st.column_config.NumberColumn('Days Present', format="%d"),
                'Spiel Key': st.column_config.TextColumn('Spiel Config')
            }

        st.dataframe(
            display_df,
            use_container_width=True,
            hide_index=True,
            column_config=col_config
        )

        # Bar chart
        st.bar_chart(
            display_df.set_index('Agent')[['Opening', 'Closing']],
            color=['#3B82F6', '#10B981']
        )
    else:
        st.warning("No spiel data found for this date range.")

    st.divider()

    # Message Review section
    st.subheader("🔍 Message Review (Conversation Flow)")
    st.markdown("**Closing spiels only count if there was an Opening first in the conversation.**")

    # Reuse the conversation data from summary
    conversations = conversations_for_summary
    all_convs = all_convs_for_summary
    convs_with_spiels = set(conversations.keys())
    convs_without_spiels = set(all_convs.keys()) - convs_with_spiels

    if conversations or all_convs:
        # Count valid spiels per agent
        valid_openings = {}
        valid_closings = {}
        for conv in conversations.values():
            if conv['has_opening']:
                owner = conv['opening_owner']
                valid_openings[owner] = valid_openings.get(owner, 0) + 1
            if conv['has_closing']:
                owner = conv['closing_owner']
                valid_closings[owner] = valid_closings.get(owner, 0) + 1

        # Valid counts by agent
        with st.expander("📊 Valid Spiel Counts by Agent", expanded=True):
            agent_stats = []
            all_agents = set(valid_openings.keys()) | set(valid_closings.keys())
            for agent in sorted(all_agents):
                agent_stats.append({
                    'Agent': agent,
                    'Valid Opening': valid_openings.get(agent, 0),
                    'Valid Closing': valid_closings.get(agent, 0),
                    'Complete': min(valid_openings.get(agent, 0), valid_closings.get(agent, 0))
                })

            if agent_stats:
                stats_df = pd.DataFrame(agent_stats)
                st.dataframe(stats_df, use_container_width=True, hide_index=True)

        # Detailed Message Review
        st.subheader("📝 Detailed Message Review")

        # Chat filter - always visible
        st.markdown("##### 🔎 Chat Filter")
        filter_row1, filter_row2 = st.columns([1, 1])
        with filter_row1:
            chat_filter = st.radio(
                "Filter Chats by Spiel",
                ["With Spiels", "Without Spiels", "All Chats"],
                horizontal=True,
                key="chat_spiel_filter",
                help=f"With: {len(convs_with_spiels)} | Without: {len(convs_without_spiels)} | Total: {len(all_convs)}"
            )
        with filter_row2:
            view_mode = st.radio(
                "View Mode",
                ["Spiel Messages Only", "Full Conversation"],
                horizontal=True,
                help="Spiel Messages Only: Shows detected spiel messages. Full Conversation: Shows entire chat history."
            )

        # Filter options
        filter_col1, filter_col2, filter_col3 = st.columns(3)
        with filter_col1:
            filter_type = st.selectbox("Filter by Type", ["All", "Opening", "Closing"])
        with filter_col2:
            filter_valid = st.selectbox("Filter by Valid", ["All", "Valid Only", "Invalid Only"])
        with filter_col3:
            all_owners = sorted(set(msg['owner'] for conv in conversations.values() for msg in conv['messages'] if 'owner' in msg))
            filter_owner = st.selectbox("Filter by Owner", ["All"] + all_owners)

        if view_mode == "Full Conversation":
            # Get conversation IDs based on chat_filter selection
            if chat_filter == "All Chats":
                # Get ALL conversations (with or without spiels)
                conv_ids = list(all_convs.keys())
                st.info(f"📂 Loading ALL {len(conv_ids)} conversations (max 500)")
            elif chat_filter == "Without Spiels":
                # Only conversations WITHOUT spiels
                conv_ids = list(convs_without_spiels)
                st.info(f"📂 Loading {len(conv_ids)} conversations WITHOUT spiels")
            else:
                # Only conversations with spiels
                conv_ids = list(conversations.keys())
                st.info(f"📂 Loading {len(conv_ids)} conversations WITH spiels")

            if conv_ids:
                full_history = get_full_conversation_history(conv_ids, start_date, end_date)

                if full_history:
                    # Pagination for large datasets
                    items_per_page = 20
                    total_pages = max(1, (len(full_history) + items_per_page - 1) // items_per_page)
                    page_num = st.number_input("Page", min_value=1, max_value=total_pages, value=1, key="conv_page")
                    start_idx = (page_num - 1) * items_per_page
                    end_idx = start_idx + items_per_page

                    conv_list = list(full_history.items())[start_idx:end_idx]
                    st.caption(f"Showing {start_idx+1}-{min(end_idx, len(full_history))} of {len(full_history)} conversations")

                    # Display each conversation
                    for conv_id, conv_data in conv_list:
                        spiel_info = conversations.get(conv_id, {})
                        has_opening = spiel_info.get('has_opening', False)
                        has_closing = spiel_info.get('has_closing', False)

                        # Build header
                        status_icons = []
                        if has_opening:
                            status_icons.append("Opening")
                        if has_closing:
                            status_icons.append("Closing")
                        status = " + ".join(status_icons) if status_icons else "No Spiels"

                        with st.expander(f"📬 {conv_data['page']} - {status} ({len(conv_data['messages'])} msgs) | ID: {conv_id[:12]}...", expanded=False):
                            # Meta Business Suite link
                            page_id = conv_data.get('page_id', '')
                            if page_id:
                                meta_url = get_meta_inbox_url(page_id, conv_id)
                                st.markdown(f"🔗 [Open in Meta Business Suite]({meta_url})")
                            st.code(conv_id, language=None)
                            if spiel_info:
                                if has_opening:
                                    st.caption(f"Opening Owner: **{spiel_info.get('opening_owner', 'N/A')}**")
                                if has_closing:
                                    st.caption(f"Closing Owner: **{spiel_info.get('closing_owner', 'N/A')}**")

                            # Display all messages in conversation
                            for i, msg in enumerate(conv_data['messages']):
                                sender = "Agent" if msg['is_from_page'] else "Customer"
                                time_str = msg['time'].strftime('%Y-%m-%d %H:%M:%S') if hasattr(msg['time'], 'strftime') else str(msg['time'])

                                # Check if this message is a spiel
                                is_spiel = False
                                spiel_type = ""
                                for spiel_msg in spiel_info.get('messages', []):
                                    if spiel_msg.get('text') == msg['text']:
                                        is_spiel = True
                                        spiel_type = spiel_msg.get('type', '')
                                        break

                                spiel_badge = f" **[{spiel_type}]**" if is_spiel else ""
                                sender_icon = "🔵" if msg['is_from_page'] else "⚪"

                                st.markdown(f"{sender_icon} **{sender}**{spiel_badge} - {time_str}")
                                st.text_area(
                                    f"msg_{conv_id}_{i}",
                                    value=msg['text'],
                                    height=80,
                                    key=f"full_{conv_id}_{i}_{page_num}",
                                    disabled=True,
                                    label_visibility="collapsed"
                                )
                else:
                    st.info("No messages found in conversations.")
            else:
                st.info("No conversations found.")
        else:
            # Spiel-only view
            if chat_filter == "Without Spiels":
                # Show conversations without spiels (no spiel messages to display)
                st.warning(f"📭 Showing {len(convs_without_spiels)} conversations WITHOUT spiels. Switch to 'Full Conversation' mode to view their messages.")

                # Show list of conv IDs without spiels
                if convs_without_spiels:
                    conv_ids = list(convs_without_spiels)[:50]  # Limit to 50
                    full_history = get_full_conversation_history(conv_ids, start_date, end_date)

                    st.caption(f"Showing first {len(conv_ids)} conversations without spiels")
                    for conv_id, conv_data in list(full_history.items())[:20]:
                        with st.expander(f"📭 {conv_data['page']} ({len(conv_data['messages'])} msgs) | ID: {conv_id[:12]}...", expanded=False):
                            # Meta Business Suite link
                            page_id = conv_data.get('page_id', '')
                            if page_id:
                                meta_url = get_meta_inbox_url(page_id, conv_id)
                                st.markdown(f"🔗 [Open in Meta Business Suite]({meta_url})")
                            st.code(conv_id, language=None)
                            st.caption("No spiels detected in this conversation")
                review_data = []  # No spiel messages to show

            elif chat_filter == "All Chats":
                # Build review data for conversations with spiels only (can't show spiels from no-spiel convs)
                st.info(f"📋 Showing spiel messages from {len(conversations)} conversations WITH spiels. (Conversations without spiels have no spiel messages to display)")
                review_data = []
                for conv_id, conv in conversations.items():
                    for msg in conv['messages']:
                        is_valid = msg.get('valid', True) if msg['type'] == 'Closing' else True
                        review_data.append({
                            'conv_id': conv_id,
                            'page': conv['page'],
                            'time': msg['time'],
                            'type': msg['type'],
                            'owner': msg['owner'],
                            'score': msg['score'],
                            'valid': is_valid,
                            'message': msg['text']
                        })
            else:
                # With Spiels - original behavior
                st.info(f"📋 Showing spiel messages from {len(conversations)} conversations WITH spiels")
                review_data = []
                for conv_id, conv in conversations.items():
                    for msg in conv['messages']:
                        is_valid = msg.get('valid', True) if msg['type'] == 'Closing' else True
                        review_data.append({
                            'conv_id': conv_id,
                            'page': conv['page'],
                            'time': msg['time'],
                            'type': msg['type'],
                            'owner': msg['owner'],
                            'score': msg['score'],
                            'valid': is_valid,
                            'message': msg['text']
                        })

            if review_data:
                # Apply filters
                filtered_data = review_data
                if filter_type != "All":
                    filtered_data = [r for r in filtered_data if r['type'] == filter_type]
                if filter_valid == "Valid Only":
                    filtered_data = [r for r in filtered_data if r['valid']]
                elif filter_valid == "Invalid Only":
                    filtered_data = [r for r in filtered_data if not r['valid']]
                if filter_owner != "All":
                    filtered_data = [r for r in filtered_data if r['owner'] == filter_owner]

                st.info(f"Showing {len(filtered_data)} of {len(review_data)} spiel messages")

                # Display each message in detail
                for i, msg in enumerate(filtered_data):
                    valid_icon = "✅" if msg['valid'] else "❌"

                    with st.container():
                        col1, col2, col3, col4, col5 = st.columns([1, 1, 1, 1, 1])
                        with col1:
                            st.caption(f"**{msg['type']}** {valid_icon}")
                        with col2:
                            st.caption(f"Owner: **{msg['owner']}**")
                        with col3:
                            time_str = msg['time'].strftime('%Y-%m-%d %H:%M') if hasattr(msg['time'], 'strftime') else str(msg['time'])
                            st.caption(f"Time: {time_str}")
                        with col4:
                            st.caption(f"Match: **{msg['score']:.0%}**")
                        with col5:
                            st.caption(f"Page: {msg['page']}")

                        # Full message in a text area for easy copying
                        st.text_area(
                            f"Message #{i+1}",
                            value=msg['message'],
                            height=100,
                            key=f"spiel_{i}_{msg['conv_id']}",
                            disabled=True
                        )
                        st.divider()

        # Conversations without closing (needs follow-up)
        with st.expander("⚠️ Conversations Without Closing", expanded=False):
            unclosed = []
            for conv_id, conv in conversations.items():
                if conv['has_opening'] and not conv['has_closing']:
                    unclosed.append({
                        'Conv ID': conv_id[:12] + '...' if len(str(conv_id)) > 12 else conv_id,
                        'Page': conv['page'],
                        'Opening Owner': conv['opening_owner'],
                        'Opening Time': conv['opening_time'].strftime('%Y-%m-%d %H:%M') if hasattr(conv['opening_time'], 'strftime') else str(conv['opening_time'])[:16]
                    })

            if unclosed:
                st.warning(f"Found {len(unclosed)} conversations with opening but no closing spiel")
                st.dataframe(pd.DataFrame(unclosed), use_container_width=True, hide_index=True)
            else:
                st.success("All conversations with opening have proper closing!")
    else:
        st.info("No conversations found for this date range.")

    st.divider()

    # Spiel reference
    st.subheader("📋 Spiel Reference")

    with st.expander("View All Agent Spiels", expanded=False):
        spiel_data = []
        for agent, config in AGENT_SPIELS.items():
            spiel_data.append({
                'Agent': agent,
                'Opening Spiel': config.get('opening', ('', []))[0],
                'Closing Spiel': config.get('closing', ('', []))[0]
            })

        st.dataframe(
            pd.DataFrame(spiel_data),
            use_container_width=True,
            hide_index=True,
            column_config={
                'Agent': st.column_config.TextColumn('Agent', width='small'),
                'Opening Spiel': st.column_config.TextColumn('Opening Spiel', width='large'),
                'Closing Spiel': st.column_config.TextColumn('Closing Spiel', width='large')
            }
        )

    # Trend section
    st.divider()
    st.subheader("📈 Spiel Trend (Last 7 Days)")

    trend_df = get_spiel_trend(7)

    if not trend_df.empty:
        # Daily totals
        daily_totals = trend_df.groupby('Date').agg({
            'Opening': 'sum',
            'Closing': 'sum'
        }).reset_index()
        daily_totals['Total'] = daily_totals['Opening'] + daily_totals['Closing']

        st.line_chart(
            daily_totals.set_index('Date')[['Opening', 'Closing']],
            color=['#3B82F6', '#10B981']
        )

        # Per-agent trend
        with st.expander("View Per-Agent Trend", expanded=False):
            pivot_opening = trend_df.pivot(index='Date', columns='Agent', values='Opening').fillna(0)
            st.write("**Opening Spiels by Agent:**")
            st.dataframe(pivot_opening)

            pivot_closing = trend_df.pivot(index='Date', columns='Agent', values='Closing').fillna(0)
            st.write("**Closing Spiels by Agent:**")
            st.dataframe(pivot_closing)
    else:
        st.info("No trend data available yet.")


if __name__ == "__main__":
    main()
