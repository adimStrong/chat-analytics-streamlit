"""
Centralized configuration for Chat Analytics Dashboard
All constants, color schemes, and business logic definitions
"""

# ============================================
# CORE PAGE DEFINITIONS
# ============================================
# Only these 7 pages should be included in reports (Management Requirement)
CORE_PAGES = [
    "Juan365",
    "Juanbingo",
    "Juancares",
    "Juan365 Live Stream",
    "Juan365 Livestream",
    "Juansports",
    "Juan365 Studios"
]

# SQL-ready tuple for WHERE IN clauses
CORE_PAGES_SQL = tuple(CORE_PAGES)

# ============================================
# PAGE CATEGORIES (for toggle filter)
# ============================================
# Live Stream pages (with "Live Stream" in name)
LIVESTREAM_PAGES = [
    "Juan365 Live Stream",
    "Juan365 Livestream"
]
LIVESTREAM_PAGES_SQL = tuple(LIVESTREAM_PAGES)

# Social Media pages (without Live Stream)
SOCMED_PAGES = [
    "Juan365",
    "Juanbingo",
    "Juancares",
    "Juan365 Cares",
    "Juansports",
    "Juan365 Studios"
]
SOCMED_PAGES_SQL = tuple(SOCMED_PAGES)

# ============================================
# TIMEZONE CONFIGURATION
# ============================================
TIMEZONE = "Asia/Manila"
TIMEZONE_OFFSET = "+08:00"

# ============================================
# SHIFT DEFINITIONS
# ============================================
SHIFTS = {
    "Morning": {
        "start_hour": 6,
        "end_hour": 13,
        "display_name": "Morning (6am-2pm)",
        "sql_condition": "BETWEEN 6 AND 13",
        "order": 1
    },
    "Mid": {
        "start_hour": 14,
        "end_hour": 21,
        "display_name": "Mid (2pm-10pm)",
        "sql_condition": "BETWEEN 14 AND 21",
        "order": 2
    },
    "Evening": {
        "start_hour": 22,
        "end_hour": 5,
        "display_name": "Evening (10pm-6am)",
        "sql_condition": "NOT BETWEEN 6 AND 21",
        "order": 3
    }
}

# Ordered list for display/iteration
SHIFT_ORDER = ["Morning", "Mid", "Evening"]

# ============================================
# CACHE SETTINGS (in seconds)
# ============================================
CACHE_TTL = {
    "default": 60,           # 1 minute for most data queries
    "date_range": 300,       # 5 minutes for date range (rarely changes)
    "static_data": 600,      # 10 minutes for agent lists, page assignments
}

# ============================================
# COLOR SCHEMES
# ============================================
COLORS = {
    # Chart colors
    "primary": "#3B82F6",      # Blue - Messages Received
    "secondary": "#10B981",    # Green - Messages Sent
    "accent": "#8B5CF6",       # Purple - Unique Convos
    "highlight": "#EC4899",    # Pink - Comments

    # Status colors (for agent status styling)
    "status": {
        "present": {
            "background": "#d1fae5",
            "text": "#065f46"
        },
        "absent": {
            "background": "#fee2e2",
            "text": "#991b1b"
        },
        "off": {
            "background": "#f3f4f6",
            "text": "#4b5563"
        }
    }
}

# ============================================
# DISPLAY SETTINGS
# ============================================
DISPLAY = {
    "date_format": "%Y-%m-%d",
    "date_display_format": "%b %d, %Y",
    "datetime_format": "%Y-%m-%d %H:%M:%S",
    "default_limit": 50,
    "max_limit": 100
}

# ============================================
# RESPONSE TIME THRESHOLDS
# ============================================
# Responses under this threshold (in seconds) are considered automated/bot
BOT_RESPONSE_THRESHOLD = 10

# ============================================
# ALERT THRESHOLDS
# ============================================
ALERT_THRESHOLDS = {
    # Response rate alerts (percentage)
    "response_rate": {
        "critical": 30,       # Below 30% - critical
        "warning": 50,        # Below 50% - warning
        "good": 80            # Above 80% - good
    },
    # Response time alerts (seconds)
    "response_time": {
        "critical": 3600,     # Above 60 min - critical
        "warning": 1800,      # Above 30 min - warning
        "good": 600           # Below 10 min - good
    },
    # Message volume change alerts (percentage change from previous period)
    "volume_change": {
        "critical_drop": -50,  # Dropped more than 50%
        "warning_drop": -25,   # Dropped more than 25%
        "warning_spike": 100,  # Increased more than 100%
    },
    # Attendance alerts
    "attendance": {
        "critical": 70,       # Below 70% attendance - critical
        "warning": 85,        # Below 85% attendance - warning
    },
    # Agent absence threshold (days)
    "absence_days": {
        "critical": 3,        # More than 3 consecutive days
        "warning": 2          # More than 2 consecutive days
    }
}

# Alert severity levels and their display properties
ALERT_SEVERITY = {
    "critical": {
        "icon": "🔴",
        "color": "#dc2626",
        "background": "#fef2f2",
        "priority": 1
    },
    "warning": {
        "icon": "🟡",
        "color": "#d97706",
        "background": "#fffbeb",
        "priority": 2
    },
    "info": {
        "icon": "🔵",
        "color": "#2563eb",
        "background": "#eff6ff",
        "priority": 3
    }
}

# ============================================
# SPILL DETECTION (Closing Message Keywords)
# ============================================
# Keywords that indicate a conversation was properly closed (case-insensitive)
# Spill tracking starts from January 11, 2026
SPILL_START_DATE = "2026-01-11"

SPILL_KEYWORDS = [
    "good luck po",
    "play responsibly",
    "thank you for reaching out",
    "happy to assist",
    "don't hesitate to contact",
    "juankada",
    "stay in control",
    "play smart",
    "let us know if you need",
    "play only what you can afford",
    "gaming should be fun"
]

# ============================================
# QA SCORING WEIGHTS (Industry Standard)
# ============================================
QA_WEIGHTS = {
    'response_time': 0.40,      # 40% - How fast agents respond
    'resolution_rate': 0.35,    # 35% - % of conversations closed with spill keywords
    'productivity': 0.25        # 25% - Messages sent per day vs team average
}

# Response Time Scoring Thresholds (in seconds)
QA_RESPONSE_THRESHOLDS = {
    'excellent': {'max_seconds': 300, 'score': 100},    # < 5 min = 100 pts
    'good': {'max_seconds': 900, 'score': 80},          # 5-15 min = 80 pts
    'average': {'max_seconds': 1800, 'score': 60},      # 15-30 min = 60 pts
    'below_average': {'max_seconds': 3600, 'score': 40}, # 30-60 min = 40 pts
    'poor': {'max_seconds': float('inf'), 'score': 20}  # > 60 min = 20 pts
}
