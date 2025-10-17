"""
Utility script to drop (delete) a PostgreSQL table by name.
- Uses existing db_connect.py for connection.
- Accepts a table name as an argument.
- Confirms and safely executes the DROP TABLE statement.

Example: python3 -m apps.tools.db_deleteTable meter_data
"""

import sys
from apps.utils.db_connect import get_connection


def drop_table(table_name: str, cascade: bool = False):
    """
    Drop a PostgreSQL table by name.
    :param table_name: name of the table to drop
    :param cascade: if True, also remove dependent objects (views, FKs, etc.)
    """
    if not table_name or ";" in table_name:
        raise ValueError("Invalid table name (empty or contains illegal characters).")

    drop_sql = f"DROP TABLE IF EXISTS {table_name} {'CASCADE' if cascade else ''};"

    conn = get_connection()  # will use default server
    cur = conn.cursor()
    try:
        cur.execute(drop_sql)
        conn.commit()
        print(f"Table '{table_name}' dropped successfully.")
    except Exception as e:
        print(f"Failed to drop table '{table_name}': {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    # Example: python3 -m apps.tools.db_deleteTable meter_data
    if len(sys.argv) < 2:
        print("Usage: python3 -m apps.tools.db_deleteTable <table_name> [--cascade]")
        sys.exit(1)

    table = sys.argv[1]
    cascade_flag = "--cascade" in sys.argv

    drop_table(table, cascade=cascade_flag)
