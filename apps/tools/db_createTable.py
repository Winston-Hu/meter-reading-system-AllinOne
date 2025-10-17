from apps.utils.db_connect import get_connection

# List of all table columns (name, SQL type)
TABLE_COLUMNS = [
    ("uuid_event_id", "UUID PRIMARY KEY"),
    ("meter_serial", "TEXT NOT NULL"),
    ("site", "TEXT"),
    ("level", "TEXT"),
    ("location_id", "TEXT"),
    ("special_id", "TEXT"),
    ("dev_eui", "TEXT"),
    ("end_node_id", "INT"),
    ("reading", "NUMERIC(12,3)"),
    ("timestamp", "TIMESTAMPTZ"),
    ("rssi", "INT"),
    ("snr", "NUMERIC(6,2)"),
    ("spreading_factor", "INT"),
    ("gateway_id", "TEXT"),
    ("fcnt", "BIGINT"),
    ("litter_factor", "TEXT"),
    ("nmi", "TEXT"),
    ("misc3", "TEXT")
]


def create_table(table_name="meter_data"):
    """
    Create a PostgreSQL table dynamically from TABLE_COLUMNS list.
    If the table already exists, it will not be recreated.
    """
    column_defs = ",\n  ".join([f"{name} {dtype}" for name, dtype in TABLE_COLUMNS])
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
      {column_defs}
    );
    """
    conn = get_connection()  # will use default server
    cur = conn.cursor()
    cur.execute(create_sql)
    conn.commit()
    cur.close()
    conn.close()
    print(f"✅ Table '{table_name}' created or already exists.")


if __name__ == "__main__":
    create_table()
