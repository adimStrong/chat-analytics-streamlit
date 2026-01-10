"""
Database utilities for Chat Analytics Dashboard
Handles connections, error handling, and base query patterns
"""

import os
import streamlit as st
import psycopg2
from psycopg2 import pool
from contextlib import contextmanager
from functools import wraps
import pandas as pd

# Load .env file (for local development)
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # python-dotenv not installed, rely on environment variables


# ============================================
# DATABASE URL RESOLUTION
# ============================================
def get_database_url() -> str:
    """
    Get database URL with priority:
    1. Streamlit secrets (production on Streamlit Cloud)
    2. Environment variable from .env (local development)
    3. Raise error if neither found
    """
    # Try Streamlit secrets first (production)
    try:
        if hasattr(st, 'secrets') and 'DATABASE_URL' in st.secrets:
            return st.secrets['DATABASE_URL']
    except Exception:
        pass

    # Try environment variable (local dev with .env)
    db_url = os.getenv('DATABASE_URL')
    if db_url:
        return db_url

    # No credentials found
    raise ValueError(
        "DATABASE_URL not found. Please set it in:\n"
        "  - .streamlit/secrets.toml (for Streamlit Cloud)\n"
        "  - .env file (for local development)"
    )


# ============================================
# CONNECTION POOL (singleton pattern)
# ============================================
_connection_pool = None


def get_connection_pool():
    """
    Create a thread-safe connection pool.
    Uses module-level singleton to persist across Streamlit reruns.
    """
    global _connection_pool

    if _connection_pool is None:
        try:
            db_url = get_database_url()
            _connection_pool = psycopg2.pool.ThreadedConnectionPool(
                minconn=1,
                maxconn=10,
                dsn=db_url
            )
        except Exception as e:
            st.error(f"Failed to create connection pool: {e}")
            raise

    return _connection_pool


@contextmanager
def get_connection():
    """
    Context manager for database connections.
    Automatically returns connection to pool on exit.

    Usage:
        with get_connection() as conn:
            cur = conn.cursor()
            cur.execute(...)
            results = cur.fetchall()
            cur.close()
    """
    pool = get_connection_pool()
    conn = None
    try:
        conn = pool.getconn()
        yield conn
    except psycopg2.Error as e:
        st.error(f"Database error: {e}")
        raise
    finally:
        if conn:
            pool.putconn(conn)


# ============================================
# SIMPLE CONNECTION (for compatibility)
# ============================================
def get_simple_connection():
    """
    Get a simple database connection (not pooled).
    Use this for backwards compatibility during migration.
    Remember to close the connection when done!
    """
    return psycopg2.connect(get_database_url())


# ============================================
# ERROR HANDLING DECORATOR
# ============================================
def handle_db_errors(func):
    """
    Decorator for database query functions.
    Provides consistent error handling and logging.
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except psycopg2.OperationalError as e:
            st.error(f"Database connection error: {e}")
            st.info("Please try refreshing the page.")
            return None
        except psycopg2.ProgrammingError as e:
            st.error(f"Query error: {e}")
            return None
        except Exception as e:
            st.error(f"Unexpected error: {e}")
            return None
    return wrapper


# ============================================
# BASE QUERY EXECUTORS
# ============================================
@handle_db_errors
def execute_query(query: str, params: tuple = None, fetch: str = "all"):
    """
    Execute a query with proper connection management.

    Args:
        query: SQL query string
        params: Query parameters (tuple)
        fetch: "all", "one", or "none" (for INSERT/UPDATE)

    Returns:
        Query results or None on error
    """
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(query, params)

        if fetch == "all":
            result = cur.fetchall()
        elif fetch == "one":
            result = cur.fetchone()
        else:
            result = None
            conn.commit()

        cur.close()
        return result


def execute_query_df(query: str, params: tuple = None, columns: list = None) -> pd.DataFrame:
    """
    Execute query and return as pandas DataFrame.

    Args:
        query: SQL query string
        params: Query parameters
        columns: Column names for DataFrame

    Returns:
        pandas DataFrame or empty DataFrame on error
    """
    result = execute_query(query, params, fetch="all")

    if result is None:
        return pd.DataFrame(columns=columns or [])

    return pd.DataFrame(result, columns=columns)


# ============================================
# HEALTH CHECK
# ============================================
def check_connection() -> bool:
    """Test database connectivity."""
    try:
        result = execute_query("SELECT 1", fetch="one")
        return result is not None and result[0] == 1
    except Exception:
        return False
