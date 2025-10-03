"""
Database utilities for SQLite operations.

Provides connection management and schema initialization for the saver service.
"""

import logging
import sqlite3
from pathlib import Path

from utils.config import settings

logger = logging.getLogger(__name__)


def get_conn() -> sqlite3.Connection:
    """
    Get SQLite database connection with dict-friendly row factory.

    Returns:
        SQLite connection with row_factory set to sqlite3.Row

    Raises:
        sqlite3.Error: If connection fails
    """
    # Ensure database directory exists
    db_path = Path(settings.SQLITE_PATH)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(settings.SQLITE_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_schema() -> None:
    """
    Initialize database schema by creating required tables if they don't exist.

    Creates:
    - processed_users: for storing processed user records
    - dlq_users: for storing dead letter queue records

    Raises:
        sqlite3.Error: If schema creation fails
    """
    with get_conn() as conn:
        # Create raw_users table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS raw_users (
                id INTEGER,
                inserted_at TEXT NOT NULL,
                data TEXT NOT NULL
            )
        """)

        # Create processed_users table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS processed_users (
                id INTEGER,
                inserted_at TEXT NOT NULL,
                data TEXT NOT NULL,
                department_code TEXT
            )
        """)

        # Create dlq_users table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS dlq_users (
                id INTEGER,
                inserted_at TEXT NOT NULL,
                error_reason TEXT NOT NULL,
                data TEXT NOT NULL
            )
        """)

        conn.commit()

    logger.info("DB schema ready")