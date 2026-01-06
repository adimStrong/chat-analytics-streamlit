"""
Chat Analytics Dashboard
SMA Report and T+1 Daily Report
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

- **📊 SMA Report** - Social Media Agent daily performance tracking
- **📅 T1 Report** - T+1 Daily summary with date range support

""")

st.info("👈 Select a report from the sidebar to get started.")
