"""
Chat Analytics Dashboard
Executive Dashboard, T+1 Report, and Leaderboard
"""

import streamlit as st
from config import LIVESTREAM_PAGES_SQL, SOCMED_PAGES_SQL, CORE_PAGES_SQL

st.set_page_config(
    page_title="Chat Analytics Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ============================================
# GLOBAL PAGE FILTER TOGGLE
# ============================================
st.sidebar.markdown("### Data Filter")
page_category = st.sidebar.radio(
    "Select Pages",
    ["All Pages", "Live Stream", "Socmed"],
    key="page_category_filter",
    help="Filter all reports by page category"
)

# Store the SQL-ready tuple in session state for use across pages
if page_category == "Live Stream":
    st.session_state['page_filter_sql'] = LIVESTREAM_PAGES_SQL
    st.session_state['page_filter_name'] = "Live Stream"
elif page_category == "Socmed":
    st.session_state['page_filter_sql'] = SOCMED_PAGES_SQL
    st.session_state['page_filter_name'] = "Socmed"
else:
    st.session_state['page_filter_sql'] = CORE_PAGES_SQL
    st.session_state['page_filter_name'] = "All Pages"

st.sidebar.markdown("---")

st.title("📊 Chat Analytics Dashboard")
st.markdown("---")

st.markdown("""
### Available Reports

Use the sidebar to navigate between reports:

- **🏠 Executive Dashboard** - High-level KPIs, trends, and alerts
- **📅 T1 Report** - T+1 Daily summary with date range support
- **🏆 Leaderboard** - Agent rankings and performance scores

""")

st.info("👈 Select a report from the sidebar to get started.")
