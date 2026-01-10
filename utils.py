"""
Utility functions for Chat Analytics Dashboard
Formatting, styling, and display helpers
"""

import pandas as pd
from config import COLORS


# ============================================
# NUMBER FORMATTING
# ============================================
def format_number(val):
    """
    Format number with comma separator.
    Handles None, NaN, integers, and floats.

    Examples:
        format_number(1234) -> "1,234"
        format_number(1234.5) -> "1,234.5"
        format_number(None) -> "N/A"
    """
    if pd.isna(val) or val is None:
        return "N/A"
    if isinstance(val, (int, float)):
        if val == int(val):
            return f"{int(val):,}"
        return f"{val:,.1f}"
    return str(val)


def format_dataframe_numbers(df, exclude_cols=None):
    """
    Apply comma formatting to all numeric columns in a DataFrame.

    Args:
        df: pandas DataFrame
        exclude_cols: List of column names to skip formatting

    Returns:
        New DataFrame with formatted values (strings)
    """
    if exclude_cols is None:
        exclude_cols = []

    df_formatted = df.copy()
    numeric_dtypes = ['int64', 'float64', 'int32', 'float32', 'Int64', 'Float64']

    for col in df_formatted.columns:
        if col not in exclude_cols and str(df_formatted[col].dtype) in numeric_dtypes:
            df_formatted[col] = df_formatted[col].apply(format_number)

    return df_formatted


# ============================================
# RESPONSE TIME FORMATTING
# ============================================
def format_rt(seconds):
    """
    Format response time in seconds to human-readable string.

    Examples:
        format_rt(45) -> "45s"
        format_rt(125) -> "2m 5s"
        format_rt(3725) -> "1h 2m"
        format_rt(None) -> "-"
    """
    if pd.isna(seconds) or seconds is None or seconds == 0:
        return "-"

    seconds = float(seconds)

    if seconds < 60:
        return f"{int(seconds)}s"
    elif seconds < 3600:
        mins = int(seconds // 60)
        secs = int(seconds % 60)
        return f"{mins}m {secs}s" if secs > 0 else f"{mins}m"
    else:
        hrs = int(seconds // 3600)
        mins = int((seconds % 3600) // 60)
        return f"{hrs}h {mins}m" if mins > 0 else f"{hrs}h"


def format_rt_short(seconds):
    """
    Shorter format for response time (used in tables with limited space).

    Examples:
        format_rt_short(45) -> "45s"
        format_rt_short(125) -> "2.1m"
        format_rt_short(3725) -> "1.0h"
    """
    if pd.isna(seconds) or seconds is None or seconds == 0:
        return "-"

    seconds = float(seconds)

    if seconds < 60:
        return f"{int(seconds)}s"
    elif seconds < 3600:
        return f"{seconds/60:.1f}m"
    else:
        return f"{seconds/3600:.1f}h"


# ============================================
# STATUS STYLING
# ============================================
def style_status(val):
    """
    Return CSS style string for status values.
    Used with DataFrame.style.applymap().

    Args:
        val: Status value ('present', 'absent', 'off')

    Returns:
        CSS style string
    """
    status_colors = COLORS.get("status", {})

    if val == 'present':
        colors = status_colors.get("present", {})
        return f"background-color: {colors.get('background', '#d1fae5')}; color: {colors.get('text', '#065f46')}"
    elif val == 'absent':
        colors = status_colors.get("absent", {})
        return f"background-color: {colors.get('background', '#fee2e2')}; color: {colors.get('text', '#991b1b')}"
    elif val == 'off':
        colors = status_colors.get("off", {})
        return f"background-color: {colors.get('background', '#f3f4f6')}; color: {colors.get('text', '#4b5563')}"
    return ''


def get_status_emoji(status):
    """Get emoji for status value."""
    emoji_map = {
        'present': '',
        'absent': '',
        'off': ''
    }
    return emoji_map.get(status, '')


# ============================================
# PERCENTAGE FORMATTING
# ============================================
def format_percentage(val, decimals=1):
    """
    Format value as percentage string.

    Args:
        val: Numeric value (already as percentage, not decimal)
        decimals: Decimal places to show

    Returns:
        Formatted percentage string
    """
    if pd.isna(val) or val is None:
        return "N/A"
    return f"{val:.{decimals}f}%"


# ============================================
# DATE FORMATTING
# ============================================
def format_date_display(date_val, include_day=True):
    """
    Format date for display in reports.

    Args:
        date_val: Date object or string
        include_day: Whether to include day name

    Returns:
        Formatted date string
    """
    if pd.isna(date_val) or date_val is None:
        return "N/A"

    if isinstance(date_val, str):
        date_val = pd.to_datetime(date_val).date()

    if include_day:
        return date_val.strftime('%Y-%m-%d (%a)')
    return date_val.strftime('%Y-%m-%d')


# ============================================
# DATAFRAME STYLING HELPERS
# ============================================
def apply_status_styling(df, status_column='Status'):
    """
    Apply status styling to a DataFrame for display.

    Args:
        df: pandas DataFrame
        status_column: Name of the status column

    Returns:
        Styled DataFrame
    """
    if status_column not in df.columns:
        return df

    return df.style.applymap(style_status, subset=[status_column])
