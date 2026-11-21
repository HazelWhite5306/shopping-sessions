# Shopping Sessions

A Rust pipeline for the Databricks Shopping Sessions track. It reads the click-stream CSVs in
`DATA_DIR`, follows `rules.md` exactly, and writes `user_daily` and `campaign_daily` as Parquet
to `OUTPUT_DIR`.

## How it works

1. Memory-map every CSV and parse fixed-size chunks in parallel (rayon).
2. Dedup events on `event_id` by smallest `ingest_ts`, then validate the kept copy.
3. Join linked IDs with union-find; each person is named by their byte-wise smallest ID.
4. Sort live events by (person, ts) and clicks by (person, ts, click_id).
5. Walk each person's events into sessions, credit the latest click within 7 days
   (smallest `click_id` on ties), count distinct assisting campaigns, and aggregate.

Click dedup is skipped: copies have identical values, so they can't change any credit or assist.

## Local check

    make test                      # unit tests
    make check-dev DEV=../dev ANSWER=../answer
    make docker

`tools/check.py` needs `duckdb` (`pip install duckdb`).
