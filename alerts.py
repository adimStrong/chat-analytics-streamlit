"""
Alert system for Chat Analytics Dashboard
Functions for generating, categorizing, and displaying alerts
"""

import streamlit as st
import pandas as pd
from datetime import date, timedelta
from config import ALERT_THRESHOLDS, ALERT_SEVERITY, CORE_PAGES_SQL, CACHE_TTL
from db_utils import get_simple_connection as get_connection
from utils import format_rt


# ============================================
# ALERT GENERATION FUNCTIONS
# ============================================

def check_response_rate_alert(rate, agent_name=None, context=None):
    """Check if response rate triggers an alert"""
    thresholds = ALERT_THRESHOLDS["response_rate"]

    if rate < thresholds["critical"]:
        return {
            "severity": "critical",
            "type": "response_rate",
            "message": f"Critical: Response rate at {rate:.1f}%",
            "agent": agent_name,
            "context": context,
            "value": rate
        }
    elif rate < thresholds["warning"]:
        return {
            "severity": "warning",
            "type": "response_rate",
            "message": f"Warning: Response rate at {rate:.1f}%",
            "agent": agent_name,
            "context": context,
            "value": rate
        }
    return None


def check_response_time_alert(seconds, agent_name=None, context=None):
    """Check if response time triggers an alert"""
    if seconds is None or seconds <= 0:
        return None

    thresholds = ALERT_THRESHOLDS["response_time"]
    rt_display = format_rt(seconds)

    if seconds > thresholds["critical"]:
        return {
            "severity": "critical",
            "type": "response_time",
            "message": f"Critical: Avg response time is {rt_display}",
            "agent": agent_name,
            "context": context,
            "value": seconds
        }
    elif seconds > thresholds["warning"]:
        return {
            "severity": "warning",
            "type": "response_time",
            "message": f"Warning: Avg response time is {rt_display}",
            "agent": agent_name,
            "context": context,
            "value": seconds
        }
    return None


def check_volume_change_alert(current, previous, metric_name="messages"):
    """Check if volume change triggers an alert"""
    if previous is None or previous == 0:
        return None

    change_pct = ((current - previous) / previous) * 100
    thresholds = ALERT_THRESHOLDS["volume_change"]

    if change_pct <= thresholds["critical_drop"]:
        return {
            "severity": "critical",
            "type": "volume_change",
            "message": f"Critical: {metric_name.title()} dropped {abs(change_pct):.1f}% vs previous period",
            "value": change_pct
        }
    elif change_pct <= thresholds["warning_drop"]:
        return {
            "severity": "warning",
            "type": "volume_change",
            "message": f"Warning: {metric_name.title()} dropped {abs(change_pct):.1f}% vs previous period",
            "value": change_pct
        }
    elif change_pct >= thresholds["warning_spike"]:
        return {
            "severity": "info",
            "type": "volume_change",
            "message": f"Notice: {metric_name.title()} increased {change_pct:.1f}% vs previous period",
            "value": change_pct
        }
    return None


def check_attendance_alert(attendance_rate, agent_name=None):
    """Check if attendance rate triggers an alert"""
    thresholds = ALERT_THRESHOLDS["attendance"]

    if attendance_rate < thresholds["critical"]:
        return {
            "severity": "critical",
            "type": "attendance",
            "message": f"Critical: Attendance at {attendance_rate:.1f}%",
            "agent": agent_name,
            "value": attendance_rate
        }
    elif attendance_rate < thresholds["warning"]:
        return {
            "severity": "warning",
            "type": "attendance",
            "message": f"Warning: Attendance at {attendance_rate:.1f}%",
            "agent": agent_name,
            "value": attendance_rate
        }
    return None


# ============================================
# DATABASE ALERT QUERIES
# ============================================

@st.cache_data(ttl=CACHE_TTL["default"])
def get_agent_performance_alerts(start_date, end_date):
    """Get agents with performance issues"""
    conn = get_connection()
    cur = conn.cursor()

    alerts = []

    # Query for performance issues
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
                THEN (100.0 * ads.messages_sent / ads.messages_received)
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

    for row in rows:
        agent_name, alert_date, shift, status, msg_recv, msg_sent, response_rate, avg_rt = row
        context = f"{alert_date} - {shift}"

        # Absence alert
        if status == 'absent':
            alerts.append({
                "severity": "warning",
                "type": "absence",
                "message": f"Agent absent",
                "agent": agent_name,
                "context": context,
                "date": alert_date
            })

        # Low response rate
        if msg_recv > 10 and response_rate < 50:
            alert = check_response_rate_alert(response_rate, agent_name, context)
            if alert:
                alert["date"] = alert_date
                alerts.append(alert)

        # Slow response time
        if avg_rt and avg_rt > 1800:
            alert = check_response_time_alert(avg_rt, agent_name, context)
            if alert:
                alert["date"] = alert_date
                alerts.append(alert)

    cur.close()
    conn.close()

    return alerts


@st.cache_data(ttl=CACHE_TTL["default"])
def get_page_alerts(start_date, end_date):
    """Get page-level performance alerts"""
    conn = get_connection()
    cur = conn.cursor()

    alerts = []

    cur.execute("""
        SELECT
            p.page_name,
            COUNT(*) FILTER (WHERE m.is_from_page = false) as msg_recv,
            COUNT(*) FILTER (WHERE m.is_from_page = true) as msg_sent,
            CASE
                WHEN COUNT(*) FILTER (WHERE m.is_from_page = false) > 0
                THEN (100.0 * COUNT(*) FILTER (WHERE m.is_from_page = true) /
                      COUNT(*) FILTER (WHERE m.is_from_page = false))
                ELSE 0
            END as response_rate
        FROM messages m
        JOIN pages p ON m.page_id = p.page_id
        WHERE (m.message_time AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')::date BETWEEN %s AND %s
          AND p.page_name IN %s
        GROUP BY p.page_name
        HAVING COUNT(*) FILTER (WHERE m.is_from_page = false) > 0
    """, (start_date, end_date, CORE_PAGES_SQL))

    for row in cur.fetchall():
        page_name, msg_recv, msg_sent, response_rate = row
        alert = check_response_rate_alert(response_rate, context=f"Page: {page_name}")
        if alert:
            alert["page"] = page_name
            alerts.append(alert)

    cur.close()
    conn.close()

    return alerts


# ============================================
# ALERT SUMMARY FUNCTIONS
# ============================================

def summarize_alerts(alerts):
    """Summarize alerts by severity and type"""
    summary = {
        "critical": [],
        "warning": [],
        "info": []
    }

    for alert in alerts:
        severity = alert.get("severity", "info")
        if severity in summary:
            summary[severity].append(alert)

    # Sort by priority
    for severity in summary:
        summary[severity].sort(key=lambda x: x.get("date", date.min) if isinstance(x.get("date"), date) else date.min, reverse=True)

    return summary


def get_alert_counts(alerts):
    """Get counts by severity"""
    counts = {"critical": 0, "warning": 0, "info": 0}
    for alert in alerts:
        severity = alert.get("severity", "info")
        if severity in counts:
            counts[severity] += 1
    return counts


# ============================================
# STREAMLIT DISPLAY FUNCTIONS
# ============================================

def display_alert_badge(severity, count):
    """Display a badge for alert count"""
    if count == 0:
        return

    props = ALERT_SEVERITY.get(severity, ALERT_SEVERITY["info"])
    icon = props["icon"]

    if severity == "critical":
        st.error(f"{icon} {count} critical issue(s)")
    elif severity == "warning":
        st.warning(f"{icon} {count} warning(s)")
    else:
        st.info(f"{icon} {count} notice(s)")


def display_alerts_summary(alerts):
    """Display a summary of all alerts"""
    counts = get_alert_counts(alerts)

    col1, col2, col3 = st.columns(3)
    with col1:
        if counts["critical"] > 0:
            st.error(f"🔴 {counts['critical']} Critical")
        else:
            st.success("✓ No critical issues")
    with col2:
        if counts["warning"] > 0:
            st.warning(f"🟡 {counts['warning']} Warnings")
        else:
            st.success("✓ No warnings")
    with col3:
        if counts["info"] > 0:
            st.info(f"🔵 {counts['info']} Notices")


def display_alerts_list(alerts, max_display=10):
    """Display alerts as a list"""
    if not alerts:
        st.success("No alerts - all metrics within normal range")
        return

    summary = summarize_alerts(alerts)

    # Critical first
    for severity in ["critical", "warning", "info"]:
        for alert in summary[severity][:max_display]:
            props = ALERT_SEVERITY.get(severity, ALERT_SEVERITY["info"])
            icon = props["icon"]

            msg = alert.get("message", "Unknown alert")
            agent = alert.get("agent")
            context = alert.get("context")

            display_text = f"{icon} {msg}"
            if agent:
                display_text = f"{icon} **{agent}**: {msg}"
            if context:
                display_text += f" ({context})"

            if severity == "critical":
                st.error(display_text)
            elif severity == "warning":
                st.warning(display_text)
            else:
                st.info(display_text)


def display_alerts_table(alerts):
    """Display alerts in a table format"""
    if not alerts:
        return

    df = pd.DataFrame(alerts)

    # Format for display
    display_cols = ['severity', 'type', 'message', 'agent', 'context']
    available_cols = [c for c in display_cols if c in df.columns]

    if available_cols:
        display_df = df[available_cols].copy()
        display_df.columns = [c.title() for c in available_cols]
        st.dataframe(display_df, hide_index=True, width="stretch")
