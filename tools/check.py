#!/usr/bin/env python3
"""Compare OUTPUT_DIR's two Parquet tables with an expected answer folder.

usage: check.py OUTPUT_DIR ANSWER_DIR
ANSWER_DIR holds user_daily.csv and campaign_daily.csv (dev-answer.zip), or
another run's user_daily/ and campaign_daily/ Parquet folders.
Exits 1 and prints sample differences when anything differs.
"""
import os
import sys

import duckdb

KEYS = {
    "user_daily": (["user_id", "day"], ["sessions", "events", "revenue_cents", "top_category"]),
    "campaign_daily": (
        ["campaign_id", "day"],
        ["channel", "sessions", "purchasing_sessions", "revenue_cents", "assisted_sessions"],
    ),
}


ORDER = {
    "user_daily": ["user_id", "day", "sessions", "events", "revenue_cents", "top_category"],
    "campaign_daily": ["campaign_id", "channel", "day", "sessions", "purchasing_sessions",
                       "revenue_cents", "assisted_sessions"],
}


def main() -> int:
    output, answer = sys.argv[1], sys.argv[2]
    con = duckdb.connect()
    failed = False
    for table, (keys, values) in KEYS.items():
        types = {"day": "DATE", "top_category": "VARCHAR", "user_id": "VARCHAR",
                 "campaign_id": "VARCHAR", "channel": "VARCHAR"}
        order = ORDER[table]
        columns = ", ".join(f"'{c}': '{types.get(c, 'BIGINT')}'" for c in order)
        if os.path.isdir(f"{answer}/{table}"):
            con.execute(
                f"CREATE TABLE want_{table} AS SELECT {', '.join(order)} "
                f"FROM read_parquet('{answer}/{table}/*.parquet')"
            )
        else:
            con.execute(
                f"CREATE TABLE want_{table} AS SELECT * FROM read_csv('{answer}/{table}.csv', "
                f"header=true, columns={{{columns}}}, nullstr='')"
            )
        con.execute(
            f"CREATE TABLE got_{table} AS SELECT {', '.join(keys + values)} "
            f"FROM read_parquet('{output}/{table}/*.parquet')"
        )
        schema = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{output}/{table}/*.parquet')").fetchall()
        names = sorted(row[0] for row in schema)
        if names != sorted(keys + values):
            print(f"{table}: columns {names}")
            failed = True
        on = " AND ".join(f"w.{k} = g.{k}" for k in keys)
        missing = con.execute(
            f"SELECT w.* FROM want_{table} w ANTI JOIN got_{table} g ON {on}").fetchall()
        extra = con.execute(
            f"SELECT g.* FROM got_{table} g ANTI JOIN want_{table} w ON {on}").fetchall()
        differ = " OR ".join(f"w.{v} IS DISTINCT FROM g.{v}" for v in values)
        differing = con.execute(
            f"SELECT w.*, '|', g.* FROM want_{table} w JOIN got_{table} g ON {on} WHERE {differ}"
        ).fetchall()
        dupes = con.execute(
            f"SELECT count(*) - count(DISTINCT ({', '.join(keys)})) FROM got_{table}").fetchone()[0]
        total = con.execute(f"SELECT count(*) FROM want_{table}").fetchone()[0]
        status = "OK" if not (missing or extra or differing or dupes) else "DIFF"
        print(f"{table}: {status} expected={total} missing={len(missing)} "
              f"extra={len(extra)} differing={len(differing)} duplicate_keys={dupes}")
        for label, rows in (("missing", missing), ("extra", extra), ("differing", differing)):
            for row in rows[:8]:
                print(f"  {label}: {row}")
        failed |= status != "OK"
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
