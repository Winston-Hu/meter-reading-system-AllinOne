"""
For one-time quick / testing scripts

Usage Examples:
Example 1 – Load default config
from apps.utils.db_connect import get_connection

conn = get_connection()
cur = conn.cursor()
cur.execute("SELECT version();")
print(cur.fetchone())
cur.close()
conn.close()

Example 2 – Override parameters manually
conn = get_connection(
    dbname="test_db",
    user="pi",
    password="1234qwer",
    host="localhost",
    port=5432,
)

Example 3 – Use custom config file
conn = get_connection(config_path="/opt/watermeter/configs/db_prod.yml")
"""


import psycopg2
import yaml
from pathlib import Path
from psycopg2.extras import RealDictCursor


def load_db_config(config_path=None):
    """
    Load database connection parameters from a YAML configuration file.
    :param config_path: optional, custom path to a YAML file.
    :return: dictionary containing database parameters.
    """
    if config_path is None:
        # Default path: project_root/configs/db.yml
        base_dir = Path(__file__).resolve().parents[2]  # e.g. /opt/watermeter/
        config_path = base_dir / "configs" / "db" / "db.yml"

    if not config_path.exists():
        raise FileNotFoundError(f"Database config file not found: {config_path}")

    with open(config_path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)

    return data.get("postgres", {})


def get_connection(
    dbname=None,
    user=None,
    password=None,
    host=None,
    port=None,
    config_path=None,
):
    """
    Establish a PostgreSQL database connection.
    Priority:
      1. Parameters passed directly in the function call.
      2. Parameters loaded from YAML configuration.
    :return: psycopg2 connection object.
    """
    cfg = load_db_config(config_path)

    params = {
        "dbname": dbname or cfg.get("dbname"),
        "user": user or cfg.get("user"),
        "password": password or cfg.get("password"),
        "host": host or cfg.get("host"),
        "port": port or cfg.get("port"),
    }

    try:
        conn = psycopg2.connect(**params)
        return conn
    except Exception as e:
        raise ConnectionError(f"❌ Database connection failed: {e}")


def get_cursor(conn):
    """
    Return a dictionary-style cursor that fetches results as dict objects.
    """
    return conn.cursor(cursor_factory=RealDictCursor)


# Optional: quick test
if __name__ == "__main__":
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT current_database(), current_user, version();")
    print(cur.fetchone())
    cur.close()
    conn.close()
