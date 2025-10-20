"""
Bulk upload mapping CSVs → PostgreSQL (one schema per site, one table per CSV).
- Scan: configs/chirpstack/*/Meter_Serial_Mapping.csv
- For each site folder:
    * schema = sanitized(site_name)
    * table  = meter_serial_mapping (overwrite each run)
    * create schema if missing
    * create table with columns from CSV header
      - PRIMARY KEY("HWMETERNMI")
      - "Channel" as INTEGER if present, others TEXT
    * COPY the CSV (HEADER TRUE)

Run from repo root:
  python -m apps.tools.db_uploadMappings_bulk
"""

from pathlib import Path
import re
import sys
import csv
import psycopg2

from apps.utils.db_connect import get_connection  # your existing connection factory

# --------- Adjust here if needed ----------
REPO_ROOT = Path(__file__).resolve().parents[2]
BASE_DIR  = (REPO_ROOT / "configs" / "chirpstack").resolve()
FILE_NAME = "Meter_Serial_Mapping.csv"
TABLE_NAME = "meter_serial_mapping"
# -----------------------------------------

sys.path.append(str(REPO_ROOT))


def site_to_schema(site_label: str) -> str:
    """Turn site folder name into a safe schema name."""
    s = site_label.strip().lower()
    s = re.sub(r"[^a-z0-9_]", "_", s)
    if not s or s[0].isdigit():
        s = "s_" + s
    return s


def discover_csvs() -> list[Path]:
    """Find all <site>/Meter_Serial_Mapping.csv under BASE_DIR."""
    if not BASE_DIR.exists():
        print(f"[warn] Base dir not found: {BASE_DIR}")
        return []
    return sorted(p for p in BASE_DIR.glob("*/" + FILE_NAME) if p.is_file())


def infer_columns(csv_path: Path) -> list[str]:
    """Read header row exactly as-is (case-sensitive)."""
    with csv_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        headers = next(reader, [])
    if not headers:
        raise ValueError(f"{csv_path}: empty header")
    return headers


def ddl_from_headers(schema: str, table: str, headers: list[str]) -> str:
    """
    Build CREATE TABLE DDL:
      - "Channel" as INTEGER if exists, others TEXT
      - PK on "HWMETERNMI" (must be present)
    """
    if "HWMETERNMI" not in headers:
        raise ValueError(f'CSV must contain column "HWMETERNMI" to be primary key. Got: {headers}')

    cols_sql = []
    for h in headers:
        if h == "Channel":
            cols_sql.append(f'"{h}" INTEGER')
        else:
            cols_sql.append(f'"{h}" TEXT')

    cols_sql.append('PRIMARY KEY("HWMETERNMI")')
    cols_joined = ", ".join(cols_sql)
    return (
        f'CREATE SCHEMA IF NOT EXISTS "{schema}";\n'
        f'DROP TABLE IF EXISTS "{schema}"."{table}";\n'
        f'CREATE TABLE "{schema}"."{table}" ({cols_joined});\n'
    )


def copy_into_table(conn, csv_path: Path, schema: str, table: str):
    """COPY CSV to target table (HEADER TRUE)."""
    with conn.cursor() as cur, csv_path.open("r", encoding="utf-8") as f:
        cur.copy_expert(
            f'COPY "{schema}"."{table}" FROM STDIN WITH (FORMAT CSV, HEADER TRUE)',
            f
        )
    conn.commit()


def process_one_csv(conn, csv_path: Path):
    site = csv_path.parent.name
    schema = site_to_schema(site)

    print(f"\n[info] site='{site}'  ->  schema='{schema}'")
    print(f"[info] csv: {csv_path}")

    headers = infer_columns(csv_path)
    ddl = ddl_from_headers(schema, TABLE_NAME, headers)

    with conn.cursor() as cur:
        cur.execute(ddl)
    conn.commit()

    copy_into_table(conn, csv_path, schema, TABLE_NAME)
    print(f"[ok] uploaded → {schema}.{TABLE_NAME}  (columns={len(headers)})")


def main():
    csv_files = discover_csvs()
    if not csv_files:
        print(f"[warn] No CSV found under: {BASE_DIR}/<site>/{FILE_NAME}")
        return

    conn = get_connection()
    try:
        for csv_path in csv_files:
            try:
                process_one_csv(conn, csv_path)
            except Exception as e:
                print(f"[error] {csv_path.name}: {e}")
        print("\nAll done.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
