#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Truncate Table
clear constant of all_meter_data.meter_events
"""

import psycopg2

DB_CONFIG = {
    "dbname": "metersmart",
    "user": "admin",
    "password": "3.1415926Pi",
    "host": "10.166.179.37",
    "port": 5432,
}


def truncate_meter_events():
    sql = "TRUNCATE TABLE all_meter_data.meter_events RESTART IDENTITY CASCADE;"
    conn = None
    try:
        print("Connecting to PostgreSQL database...")
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(sql)
            print("Successfully truncated all_meter_data.meter_events (with partitions).")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if conn:
            conn.close()
            print("Connection closed.")


if __name__ == "__main__":
    truncate_meter_events()
