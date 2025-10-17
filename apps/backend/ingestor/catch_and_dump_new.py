import json
from pathlib import Path
from typing import Any, Dict, List

from apps.utils.catch_siteWatcher import SiteWatcher
from apps.utils.catch_parserWatcher import DynamicParserManager
from apps.utils.db_pool import PostgresConnectionPool
from apps.utils.db_writer import initialize_parent, ingest_rows


PM: DynamicParserManager  # global manager instance


def handle_raw(site_name: str, topic: str, payload: bytes):
    """Decode, route by deviceProfileName, and call the dynamically loaded parser."""
    try:
        data: Dict[str, Any] = json.loads(payload.decode("utf-8"))
    except Exception as e:
        print(f"[{site_name}] JSON decode error: {e}")
        return

    profile_name = data.get("deviceInfo", {}).get("deviceProfileName")
    if not profile_name:
        print(f"[{site_name}] missing deviceProfileName; skip")
        return

    parser = PM.get_parser(profile_name)
    if not parser:
        print(f"[{site_name}] profile '{profile_name}' not configured or parser missing; skip")
        return

    try:
        rows: List[Dict[str, Any]] = parser(data, site_name.lower())

        print(f"\n[{site_name}] profile={profile_name} rows={len(rows)}")
        for r in rows[:5]:
            print("  ->", r)
        if len(rows) > 3:
            print(f"  ... and {len(rows) - 5} more rows")

        # write to Postgres (partitioned)
        inserted = ingest_rows(rows)
        if inserted:
            print(f"[{site_name}] inserted {inserted} row(s) into all_meter_data.meter_events")

    except Exception as e:
            print(f"[{site_name}] parser error for '{profile_name}': {e}")


def main():
    PostgresConnectionPool.initialize(minconn=2, maxconn=10)

    initialize_parent()  # init db writer

    repo_root = Path(__file__).resolve().parents[3]
    global PM
    PM = DynamicParserManager(repo_root=repo_root, interval_sec=10.0)

    config_root = (repo_root / "configs" / "chirpstack").resolve()
    watcher = SiteWatcher(root=config_root, interval=10.0, on_message=handle_raw)
    watcher.run_forever()


if __name__ == "__main__":
    main()
