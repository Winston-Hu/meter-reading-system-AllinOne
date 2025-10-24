# apps/utils/catch_mappingLookup.py
"""
===============================================================================
Module: catch_mappingLookup.py
Purpose:
    Provides dynamic lookup and in-memory caching for mapping metadata
    (e.g., meter_serial, address, NMI, etc.) from PostgreSQL schemas
    such as <schema>.meter_serial_mapping.

    Each uplink message produces multiple channel readings (rows), each of
    which may or may not have a corresponding mapping record in the database.
    This module fills those fields in-place by performing database lookups
    with efficient TTL-based caching.

-------------------------------------------------------------------------------
Core Function: fill_from_mapping(row)
-------------------------------------------------------------------------------
    - Enriches a single row dictionary in-place using information retrieved
      from the database table "<schema>.meter_serial_mapping".
    - Fields filled if available:
        • litter_factor      ←  litter_factor
        • nmi                ←  HWMETERNMI
        • meter_serial       ←  HWMETERNO
        • site               ←  Address
        • level              ←  Level
        • location_id        ←  ApartmentNo
    - The function only fills values that are currently None, so it never
      overwrites previously set data.

-------------------------------------------------------------------------------
Caching Mechanism
-------------------------------------------------------------------------------
    The cache key is defined as a tuple: (schema, dev_eui_upper, channel)
    Each cache entry is stored as:
        {
            "value":   <dict of mapping fields>,
            "expires": <epoch timestamp>
        }

    • HIT_TTL_SEC (default: 65 sec)
        - How long a successful lookup (HIT) remains in the cache.
        - During this period, repeated lookups for the same device/channel
          will *not* trigger a new database query.

    • MISS_TTL_SEC (default: 30 sec)
        - How long a negative result (MISS, i.e., mapping not found)
          remains cached.
        - Prevents the system from repeatedly querying the database for
          devices or channels that currently lack mapping entries.
        - After this TTL expires, the next lookup will re-query the database,
          allowing newly uploaded mapping files to be detected automatically.

-------------------------------------------------------------------------------
Behavior Summary
-------------------------------------------------------------------------------
    When cache HIT:
        - Mapping data retrieved instantly from in-memory cache.

    When cache MISS:
        - Database is queried once.
        - Result is cached with HIT_TTL_SEC or MISS_TTL_SEC based on whether
          a record was found.

    Mixed HIT/MISS scenarios:
        - Common when multi-channel devices only have partial mapping records.
        - Each channel (end_node_id) is handled independently.

-------------------------------------------------------------------------------
Example Workflow
-------------------------------------------------------------------------------
    for each row in parsed uplink:
        fill_from_mapping(row)
        → cache key = (schema, dev_eui.upper(), channel)
        → if cache valid → use cached data
        → else query DB once → refresh cache
===============================================================================
"""

from typing import Optional, Dict, Any, Tuple
from psycopg2 import sql
import time
import logging

from apps.utils.db_pool import PostgresConnectionPool
from logs.logging_setup import get_logger

HIT_TTL_SEC = 65
MISS_TTL_SEC = 30

# very small in-memory cache
_CACHE: Dict[Tuple[str, str, int], Dict[str, Optional[str]]] = {}

DEBUG = True

LOG = get_logger(
    "catch_mappingLookup",
    file_name="utils.log",
    max_bytes=5 * 1024 * 1024,
    backup_count=5,
    level=logging.INFO,
    also_console=True
)


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
            LOG.warning(f"[mapping] missing key(s): schema={schema} dev_eui={dev_eui} ch={ch}")
        return

    key = (schema, dev_eui.upper(), int(ch))
    data = _CACHE.get(key)
    if data is None:
        data = _query_mapping(schema, dev_eui, int(ch))
        _CACHE[key] = data

    if DEBUG and all(v is None for v in data.values()):
        LOG.warning(f"[mapping] MISS schema={schema} dev_eui={dev_eui} ch={ch}")

    # ---- get mapping with TTL cache ----
    data = _get_mapping_with_cache(schema, dev_eui, int(ch))

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


def _get_mapping_with_cache(schema: str, dev_eui: str, channel: int) -> Dict[str, Optional[str]]:
    """
    Fetch mapping with TTL cache for (schema, dev_eui_upper, channel).
    When cache is fresh, return cached value.
    Otherwise query DB and refresh cache with HIT_TTL_SEC or MISS_TTL_SEC.
    """
    now = time.time()
    key = (schema, dev_eui.upper(), channel)

    # Serve from cache if not expired
    entry = _CACHE.get(key)
    if entry and entry.get("expires", 0) > now:
        val = entry["value"]
        if DEBUG and val and any(val.values()):
            # cached hit
            pass
        elif DEBUG and (not val or all(v is None for v in val.values())):
            # cached miss (still within MISS_TTL)
            pass
        return val

    # Cache miss or expired -> query DB
    data = _query_mapping(schema, dev_eui, channel)

    # Decide TTL based on result
    ttl = HIT_TTL_SEC if any(data.values()) else MISS_TTL_SEC
    _CACHE[key] = {"value": data, "expires": now + ttl}

    # Logging
    if DEBUG:
        if any(data.values()):
            LOG.info(f"[mapping] HIT schema={schema} dev_eui={dev_eui} ch={channel}")
        else:
            LOG.info(f"[mapping] MISS schema={schema} dev_eui={dev_eui} ch={channel}")

    return data


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
        if DEBUG:
            LOG.exception(f"[mapping] SQL error schema={schema} dev_eui={dev_eui} ch={channel}: {e}")
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
