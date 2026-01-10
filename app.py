"""
Chat Analytics Dashboard
Executive Dashboard, T+1 Report, and Leaderboard
"""

import streamlit as st

st.set_page_config(
    page_title="Chat Analytics Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

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
