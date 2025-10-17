# apps/utils/db_writer.py
# Purpose: create schema/table if missing; ensure monthly partitions; batch-ingest rows.

from typing import List, Dict, Any, Tuple, Iterable
from datetime import datetime, timezone
from collections import defaultdict
from psycopg2 import sql
from psycopg2.extras import execute_values

from apps.utils.db_pool import PostgresConnectionPool

SCHEMA = "all_meter_data"
PARENT_TABLE = "meter_events"

# Keep the columns list in the exact insert order
COLUMNS: Tuple[str, ...] = (
    "uuid_event_id",
    "meter_serial",
    "site",
    "level",
    "location_id",
    "dev_eui",
    "end_node_id",
    "reading",
    "timestamp",
    "rssi",
    "snr",
    "spreading_factor",
    "gateway_id",
    "fcnt",
    "litter_factor",
    "nmi",
    "valve_1",
    "valve_2",
    "valve_react1",
    "is_leak",
    "battery",
    "cfg",
    "schema",
)

DDL_PARENT = f"""
CREATE SCHEMA IF NOT EXISTS {SCHEMA};

CREATE TABLE IF NOT EXISTS {SCHEMA}.{PARENT_TABLE} (
    uuid_event_id    UUID NOT NULL,
    meter_serial     TEXT,
    site             TEXT,
    level            TEXT,
    location_id      TEXT,
    dev_eui          TEXT NOT NULL,
    end_node_id      INT  NOT NULL,
    reading          NUMERIC(12,3),
    "timestamp"      TIMESTAMPTZ NOT NULL,
    rssi             INT,
    snr              NUMERIC(6,2),
    spreading_factor INT,
    gateway_id       TEXT,
    fcnt             BIGINT,
    litter_factor    TEXT,
    nmi              TEXT,
    valve_1          NUMERIC(12,3),
    valve_2          NUMERIC(12,3),
    valve_react1     NUMERIC(12,3),
    is_leak          NUMERIC(12,3),
    battery          INT,
    cfg              TEXT,
    schema           TEXT,
    PRIMARY KEY (uuid_event_id, "timestamp")
)
PARTITION BY RANGE ("timestamp");
"""

IDX_CHILD = """
CREATE INDEX IF NOT EXISTS {idx_name}
ON {schema}.{child} (dev_eui, end_node_id, "timestamp" DESC);
"""


def _month_key(ts: datetime) -> Tuple[int, int]:
    # Normalize tz; partition by UTC month to avoid ambiguity
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    else:
        ts = ts.astimezone(timezone.utc)
    return ts.year, ts.month


def _partition_name(year: int, month: int) -> str:
    return f"{PARENT_TABLE}_{year:04d}{month:02d}"


def _partition_bounds(year: int, month: int) -> Tuple[str, str]:
    # [month_start, next_month_start)
    if month == 12:
        return (f"{year:04d}-12-01 00:00:00+00", f"{year+1:04d}-01-01 00:00:00+00")
    else:
        return (f"{year:04d}-{month:02d}-01 00:00:00+00", f"{year:04d}-{month+1:02d}-01 00:00:00+00")


def initialize_parent() -> None:
    """Create schema and parent partitioned table if missing."""
    with PostgresConnectionPool.get_conn() as conn, conn.cursor() as cur:
        cur.execute(DDL_PARENT)
        conn.commit()
    print(f"ensured parent table {SCHEMA}.{PARENT_TABLE}")


def ensure_month_partition(year: int, month: int) -> None:
    child = _partition_name(year, month)
    start, end = _partition_bounds(year, month)

    with PostgresConnectionPool.get_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT 1
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = %s AND c.relname = %s
        """, (SCHEMA, child))
        exists = cur.fetchone() is not None

        if not exists:
            create_sql = sql.SQL(
                "CREATE TABLE {schema}.{child} PARTITION OF {schema}.{parent} "
                "FOR VALUES FROM (%s) TO (%s)"
            ).format(
                schema=sql.Identifier(SCHEMA),
                child=sql.Identifier(child),
                parent=sql.Identifier(PARENT_TABLE),
            )
            cur.execute(create_sql, (start, end))
            print(f"📦 created partition {SCHEMA}.{child} [{start} ~ {end})")

        idx_name = f"{child}_idx_devnode_ts"
        idx_sql = sql.SQL(
            "CREATE INDEX IF NOT EXISTS {idx} ON {schema}.{child} (dev_eui, end_node_id, \"timestamp\" DESC)"
        ).format(
            idx=sql.Identifier(idx_name),
            schema=sql.Identifier(SCHEMA),
            child=sql.Identifier(child),
        )
        cur.execute(idx_sql)

        conn.commit()


def _row_to_tuple(row: Dict[str, Any]) -> Tuple[Any, ...]:
    """Project dict to the exact column order; drop unknown keys."""
    vals = []
    for col in COLUMNS:
        v = row.get(col)
        # normalize python datetime -> tz-aware
        if col == "timestamp" and isinstance(v, datetime) and v.tzinfo is None:
            v = v.replace(tzinfo=timezone.utc)
        vals.append(v)
    return tuple(vals)


def ingest_rows(rows: List[Dict[str, Any]], batch_size: int = 1000) -> int:
    """
    Ensure partitions and batch-insert rows into parent.
    - Rows must contain at least: uuid_event_id, dev_eui, end_node_id, timestamp.
    - Unknown keys are ignored.
    - ON CONFLICT DO NOTHING on PK (uuid_event_id).
    Returns number of rows attempted (conflicts are ignored by design).
    """
    if not rows:
        return 0

    # Group rows by (year, month) for partition creation
    buckets: Dict[Tuple[int, int], List[Dict[str, Any]]] = defaultdict(list)
    for r in rows:
        ts = r.get("timestamp")
        if not isinstance(ts, datetime):
            continue
        buckets[_month_key(ts)].append(r)

    # Ensure partitions before insert
    for (y, m) in buckets.keys():
        ensure_month_partition(y, m)

    # Batch insert via execute_values into parent; routing is automatic
    col_list_sql = sql.SQL(",").join(sql.Identifier(c) for c in COLUMNS)
    insert_sql = sql.SQL("""
        INSERT INTO {schema}.{parent} ({cols})
        VALUES %s
        ON CONFLICT (uuid_event_id, "timestamp") DO NOTHING
    """).format(
        schema=sql.Identifier(SCHEMA),
        parent=sql.Identifier(PARENT_TABLE),
        cols=col_list_sql,
    )

    total = 0
    with PostgresConnectionPool.get_conn() as conn, conn.cursor() as cur:
        buf: List[Tuple[Any, ...]] = []
        for r in rows:
            buf.append(_row_to_tuple(r))
            if len(buf) >= batch_size:
                execute_values(cur, insert_sql.as_string(conn), buf)
                total += len(buf)
                buf.clear()
        if buf:
            execute_values(cur, insert_sql.as_string(conn), buf)
            total += len(buf)
        conn.commit()

    return total
