# apps/utils/catch_mappingLookup.py
from typing import Optional, Dict, Any, Tuple
from psycopg2 import sql
from apps.utils.db_pool import PostgresConnectionPool

# very small in-memory cache
_CACHE: Dict[Tuple[str, str, int], Dict[str, Optional[str]]] = {}

DEBUG = True  # 打开一次，定位问题；OK 后可关

def fill_from_mapping(row: Dict[str, Any]) -> None:
    """
    Enrich row in-place using <schema>.meter_serial_mapping.
    Required in row:
      - 'schema': str
      - 'dev_eui': str
      - 'end_node_id': int   (Channel)
    Only fills None fields; will NOT overwrite existing values.
    """
    schema = row.get("schema")
    dev_eui = row.get("dev_eui")
    ch = row.get("end_node_id")

    if not schema or not dev_eui or ch is None:
        if DEBUG:
            print(f"[mapping] missing key(s): schema={schema} dev_eui={dev_eui} ch={ch}")
        return

    key = (schema, dev_eui.upper(), int(ch))
    data = _CACHE.get(key)
    if data is None:
        data = _query_mapping(schema, dev_eui, int(ch))
        _CACHE[key] = data

    if DEBUG and all(v is None for v in data.values()):
        print(f"[mapping] MISS schema={schema} dev_eui={dev_eui} ch={ch}")

    # Only fill when None
    if row.get("litter_factor") is None:
        row["litter_factor"] = data["litter_factor"]
    if row.get("nmi") is None:
        row["nmi"] = data["nmi"]
    if row.get("meter_serial") is None:
        row["meter_serial"] = data["meter_serial"]
    if row.get("site") is None:
        row["site"] = data["site"]
    if row.get("level") is None:
        row["level"] = data["level"]
    if row.get("location_id") is None:
        row["location_id"] = data["location_id"]


def _query_mapping(schema: str, dev_eui: str, channel: int) -> Dict[str, Optional[str]]:
    """
    Robust match:
      - dev_eui: case-insensitive (UPPER(column) = UPPER(%s))
      - Channel: try to match as int (CAST) to survive text/int schema differences
    """
    qry = sql.SQL(
        'SELECT "HWMETERNMI","HWMETERNO","Address","Level","ApartmentNo","litter_factor" '
        'FROM {}.{} '
        'WHERE UPPER("LoRaWANDevEUI") = UPPER(%s) '
        '  AND (CASE WHEN pg_typeof("Channel")::text = \'integer\' THEN "Channel" = %s '
        '            ELSE ("Channel")::int = %s END) '
        'LIMIT 1'
    ).format(sql.Identifier(schema), sql.Identifier("meter_serial_mapping"))

    params = (dev_eui, channel, channel)

    try:
        with PostgresConnectionPool.get_conn() as conn, conn.cursor() as cur:
            cur.execute(qry, params)
            r = cur.fetchone()
    except Exception as e:
        # 打开调试时打印出错信息（比如 schema/表不存在）
        if DEBUG:
            print(f"[mapping] SQL error schema={schema} dev_eui={dev_eui} ch={channel}: {e}")
        r = None

    if not r:
        return {
            "nmi": None,
            "meter_serial": None,
            "site": None,
            "level": None,
            "location_id": None,
            "litter_factor": None,
        }

    nmi, meter_no, address, level, apt, litter = r
    return {
        "nmi":           nmi,
        "meter_serial":  meter_no,
        "site":          address,
        "level":         level,
        "location_id":   apt,
        "litter_factor": litter,
    }
