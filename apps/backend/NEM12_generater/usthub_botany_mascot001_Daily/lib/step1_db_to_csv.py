"""
step1_db_to_csv.py

Read rows from Postgres (schema.table = all_meter_data.meter_events),
filter by SITES and START_DATE, order as requested, and export to CSV.
"""


import os
import logging
from datetime import datetime, timedelta
import psycopg2


output_merged_csv = "data_merged/merged_messages.csv"

DAY_RANGE = 3

DB_CONFIG = {
    "dbname": "metersmart",
    "user": "admin",
    "password": "3.1415926Pi",
    "host": "10.166.179.37",
    "port": 5432,
}

# Keep ONLY rows whose site is in this list
SITES = [
    "233-255 Botany Road, Waterloo",
]

os.makedirs('log', exist_ok=True)

logging.basicConfig(
    filename='log/daily_monitor.log',
    level=logging.INFO,          # DEBUG, INFO, WARNING, ERROR, CRITICAL
    format='%(asctime)s - %(levelname)s - %(message)s'
)


def convert_date(date_str):
    """
    Convert date format (like 2024_12_31)
    """
    return datetime.strptime(date_str, "%Y_%m_%d")


def export_to_csv(start_dt: datetime, end_dt: datetime = None):
    """
    Export all columns preserving original order; replace only the "timestamp"
    column's output with ISO8601 string (YYYY-MM-DDTHH:MM:SS.US).
    """
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn, conn.cursor() as cur:
            # 1) Get columns in original order
            cur.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
            """, ("all_meter_data", "meter_events"))
            cols = [r[0] for r in cur.fetchall()]

            # 2) Build SELECT list in the SAME order; replace "timestamp" only
            select_parts = []
            for c in cols:
                if c == "timestamp":
                    select_parts.append('to_char(t."timestamp", \'YYYY-MM-DD"T"HH24:MI:SS.US\') AS "timestamp"')
                else:
                    select_parts.append(f't."{c}"')
            select_list = ", ".join(select_parts)

            # 3) WHERE (right-open end)
            if end_dt:
                where_sql = cur.mogrify(
                    'WHERE t.site = ANY(%s) AND t."timestamp" >= %s AND t."timestamp" < %s',
                    (SITES, start_dt, end_dt),
                ).decode()
            else:
                where_sql = cur.mogrify(
                    'WHERE t.site = ANY(%s) AND t."timestamp" >= %s',
                    (SITES, start_dt),
                ).decode()

            # 4) COPY (order by original timestamp column)
            copy_sql = (
                'COPY ('
                f'  SELECT {select_list} '
                '  FROM all_meter_data.meter_events AS t '
                f'  {where_sql} '
                '  ORDER BY t."timestamp" ASC, t.dev_eui, t.end_node_id'
                ') TO STDOUT WITH CSV HEADER'
            )

            os.makedirs(os.path.dirname(output_merged_csv), exist_ok=True)
            with open(output_merged_csv, "w", newline="", encoding="utf-8") as f:
                cur.copy_expert(copy_sql, f)

        logging.info("Exported CSV to %s | sites=%s | start=%s | end=%s",
                     output_merged_csv, SITES, start_dt.isoformat(),
                     end_dt.isoformat() if end_dt else "∞")
        print(f"✅ Done. CSV saved to: {output_merged_csv}")
    finally:
        conn.close()
        print("Step1 Connection closed")


def step1_main():
    # Dynamically set date
    today = datetime.now().date()  # Get the current date
    combined_end_date = today.strftime("%Y_%m_%d")  # use current date as combined_end_date
    combined_start_date = (today - timedelta(days=DAY_RANGE)).strftime("%Y_%m_%d")  # 8 days ago as combined_start_date
    print(combined_start_date, combined_end_date)
    logging.info(f"rawData start:{combined_start_date}, end:{combined_end_date}")

    start_date = convert_date(combined_start_date)
    end_date = convert_date(combined_end_date) + timedelta(days=1)

    export_to_csv(start_date, end_date)

    logging.info(f'files have been merged to {output_merged_csv}')
    print(f'files have been merged to {output_merged_csv}')


if __name__ == "__main__":
    step1_main()
