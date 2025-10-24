"""
Usage Examples:

from apps.utils.db_pool import PostgresConnectionPool

# Initialize global pool at startup
PostgresConnectionPool.initialize(minconn=2, maxconn=10)

# Safely use the database in any module
def write_data(record):
    with PostgresConnectionPool.get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO meter_data (uuid_event_id, meter_serial, reading) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING;",
            (record["uuid_event_id"], record["meter_serial"], record["reading"]),
        )
        conn.commit()

# Close the connection pool when the program exits
PostgresConnectionPool.close_all()

"""


from psycopg2 import pool
import yaml
from pathlib import Path
import psycopg2
from contextlib import contextmanager
import logging

from logs.logging_setup import get_logger


LOG = get_logger(
    "db_pool",
    file_name="utils.log",
    max_bytes=5 * 1024 * 1024,
    backup_count=5,
    level=logging.INFO,
    also_console=True
)


def load_db_config(config_path=None):
    """
    Load PostgreSQL connection parameters from a YAML config file.
    """
    if config_path is None:
        base_dir = Path(__file__).resolve().parents[2]
        config_path = base_dir / "configs" / "db" / "db.yml"

    with open(config_path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    return data.get("postgres", {})


class PostgresConnectionPool:
    """
    Manage a global PostgreSQL connection pool.
    Each worker/thread borrows a connection instead of creating a new one.
    """

    _pool = None

    @classmethod
    def initialize(cls, minconn=1, maxconn=10, config_path=None):
        """
        Initialize a global connection pool if not already created.
        """
        if cls._pool is not None:
            return cls._pool

        cfg = load_db_config(config_path)
        cls._pool = pool.SimpleConnectionPool(
            minconn,
            maxconn,
            dbname=cfg["dbname"],
            user=cfg["user"],
            password=cfg["password"],
            host=cfg["host"],
            port=cfg["port"],
        )
        LOG.info(f"Connection pool created: {minconn}–{maxconn} connections.")
        return cls._pool

    @classmethod
    @contextmanager
    def get_conn(cls):
        """
        Context manager: safely borrow and return a connection.
        Usage:
            with PostgresConnectionPool.get_conn() as conn:
                cur = conn.cursor()
                cur.execute("SELECT 1;")
        """
        if cls._pool is None:
            raise Exception("Connection pool not initialized.")

        conn = cls._pool.getconn()
        try:
            yield conn
        finally:
            cls._pool.putconn(conn)

    @classmethod
    def close_all(cls):
        """Close all pooled connections."""
        if cls._pool:
            cls._pool.closeall()
            LOG.info("All pooled connections closed.")


# Quick test
if __name__ == "__main__":
    # Initialize the pool
    PostgresConnectionPool.initialize(minconn=1, maxconn=5)

    # Borrow a connection
    with PostgresConnectionPool.get_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT version();")
        print(cur.fetchone())
        cur.close()

    # Close all connections
    PostgresConnectionPool.close_all()
