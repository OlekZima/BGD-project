#!/usr/bin/env python3
"""Benchmarks bronze ingestion into PostgreSQL: batch vs. Kafka streaming.

Modes
    direct  CSV -> server-side COPY (scripts/load_bronze.py), the "without
            Kafka" baseline.
    kafka   CSV -> producer -> Kafka -> consumers -> COPY, the streaming path.
            Consumers are spawned automatically and stopped afterwards.
    report  Renders benchmarks/RESULTS.md from the recorded runs.

Both modes ingest the same input into the same bronze tables, which are
TRUNCATED before each measured run so every run starts from zero.

Use --files N to ingest the first N raw CSVs, or --max-rows M to build a
row-capped sample (shared by both modes) for quick, fair comparisons.
"""

import argparse
import csv
import json
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import psycopg
from kafka.admin import KafkaAdminClient
from kafka.errors import KafkaError

import load_bronze
import producer
from settings import (
    KAFKA_API_VERSION,
    KAFKA_BOOTSTRAP_SERVERS,
    PROJECT_ROOT,
    RAW_DIR,
    db_connection_kwargs,
)

SCRIPTS_DIR = Path(__file__).resolve().parent
RESULTS_DIR = PROJECT_ROOT / "benchmarks"
RESULTS_FILE = RESULTS_DIR / "results.jsonl"
REPORT_FILE = RESULTS_DIR / "RESULTS.md"

CONSUMER_SCRIPTS = ("consumer_status.py", "consumer_info.py")
CONSUMER_GROUPS = ("citibike-status", "citibike-info")

POLL_INTERVAL_SECONDS = 1.0
STABLE_SECONDS = 12.0
CONSUMER_JOIN_TIMEOUT_SECONDS = 90.0
RUN_TIMEOUT_SECONDS = 3600.0


def _bronze_counts(conn: psycopg.Connection) -> tuple[int, int]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT (SELECT count(*) FROM bronze_station_status),"
            "       (SELECT count(*) FROM bronze_station_information)"
        )
        status, info = cur.fetchone()
    return status, info


def _truncate_bronze(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute("TRUNCATE bronze_station_status, bronze_station_information")
    conn.commit()
    print("Bronze tables truncated.")


def _build_sample(max_rows: int) -> Path:
    """Writes the first max_rows of the dataset to a reusable sample file.

    The .sample suffix keeps it out of the *.csv globs used elsewhere; it
    lives in data/raw so the database container can COPY it directly.
    """
    sample = RAW_DIR / f"bench_sample_{max_rows}.csv.sample"
    if sample.exists():
        return sample

    sources = sorted(RAW_DIR.glob("*.csv"))
    if not sources:
        sys.exit(f"No CSV files found in {RAW_DIR} — run dataset.py first.")

    print(f"Building sample of {max_rows:,} rows -> {sample.name}")
    written = 0
    with open(sample, "w", newline="", encoding="utf-8") as out:
        writer = None
        for source in sources:
            with open(source, newline="", encoding="utf-8") as f:
                reader = csv.reader(f)
                header = next(reader)
                if writer is None:
                    writer = csv.writer(out)
                    writer.writerow(header)
                for row in reader:
                    writer.writerow(row)
                    written += 1
                    if written >= max_rows:
                        return sample
    return sample


def _resolve_input(files: int | None, max_rows: int | None) -> list[Path]:
    if max_rows is not None:
        return [_build_sample(max_rows)]
    paths = sorted(RAW_DIR.glob("*.csv"))
    if files is not None:
        paths = paths[:files]
    if not paths:
        sys.exit(f"No CSV files found in {RAW_DIR} — run dataset.py first.")
    return paths


def _record(result: dict) -> None:
    RESULTS_DIR.mkdir(exist_ok=True)
    with open(RESULTS_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps(result) + "\n")
    print(f"\nResult appended to {RESULTS_FILE.relative_to(PROJECT_ROOT)}")
    print(json.dumps(result, indent=2))


def _make_result(mode: str, paths: list[Path], elapsed: float,
                 status_rows: int, info_rows: int, **extra) -> dict:
    total = status_rows + info_rows
    return {
        "timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "mode": mode,
        "input": [p.name for p in paths],
        "rows_status": status_rows,
        "rows_info": info_rows,
        "rows_total": total,
        "elapsed_seconds": round(elapsed, 2),
        "rows_per_second": round(total / elapsed) if elapsed > 0 else None,
        **extra,
    }


def run_direct(paths: list[Path]) -> None:
    with psycopg.connect(**db_connection_kwargs()) as conn:
        _truncate_bronze(conn)

        start = time.perf_counter()
        load_bronze.load(paths)
        elapsed = time.perf_counter() - start

        status_rows, info_rows = _bronze_counts(conn)

    _record(_make_result("direct", paths, elapsed, status_rows, info_rows))


def _ensure_topics() -> None:
    try:
        subprocess.run(
            [sys.executable, str(SCRIPTS_DIR / "create_topics.py")],
            check=True, cwd=PROJECT_ROOT,
        )
    except (subprocess.CalledProcessError, OSError) as exc:
        print(f"Warning: could not create topics ({exc}); "
              "assuming they already exist.")


def _wait_for_consumer_groups() -> None:
    admin = KafkaAdminClient(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        api_version=KAFKA_API_VERSION,
    )
    deadline = time.monotonic() + CONSUMER_JOIN_TIMEOUT_SECONDS
    try:
        while time.monotonic() < deadline:
            try:
                groups = admin.describe_consumer_groups(list(CONSUMER_GROUPS))
                if all(g.state == "Stable" and g.members for g in groups):
                    print("Consumer groups are stable.")
                    return
            except KafkaError:
                pass  # coordinator not elected yet on a fresh broker
            time.sleep(POLL_INTERVAL_SECONDS)
    finally:
        admin.close()
    sys.exit("Consumers did not join their groups in time — check the logs "
             f"in {RESULTS_DIR.relative_to(PROJECT_ROOT)}/.")


def _wait_until_counts_stable(conn: psycopg.Connection,
                              start: float | None = None) -> tuple[float, int, int]:
    """Polls bronze counts until they stop changing for STABLE_SECONDS.

    Returns (seconds from `start` to the last observed change, status, info).
    """
    origin = start if start is not None else time.monotonic()
    last_counts = _bronze_counts(conn)
    last_change = time.monotonic()
    deadline = origin + RUN_TIMEOUT_SECONDS

    while time.monotonic() < deadline:
        time.sleep(POLL_INTERVAL_SECONDS)
        counts = _bronze_counts(conn)
        now = time.monotonic()
        if counts != last_counts:
            last_counts = counts
            last_change = now
        elif now - last_change >= STABLE_SECONDS:
            return last_change - origin, *last_counts
    sys.exit("Timed out waiting for ingestion to finish.")


def run_kafka(paths: list[Path]) -> None:
    _ensure_topics()
    RESULTS_DIR.mkdir(exist_ok=True)

    consumers: list[subprocess.Popen] = []
    logs = []
    try:
        for script in CONSUMER_SCRIPTS:
            log = open(RESULTS_DIR / f"{Path(script).stem}.log", "w",
                       encoding="utf-8")
            logs.append(log)
            consumers.append(subprocess.Popen(
                [sys.executable, str(SCRIPTS_DIR / script)],
                cwd=PROJECT_ROOT, stdout=log, stderr=subprocess.STDOUT,
            ))
        print(f"Spawned {len(consumers)} consumers, waiting for group join...")
        _wait_for_consumer_groups()

        with psycopg.connect(**db_connection_kwargs()) as conn:
            print("Draining any backlog before measuring...")
            _wait_until_counts_stable(conn)
            _truncate_bronze(conn)

            start = time.monotonic()
            stats = producer.produce(files=paths)
            print(f"Producer finished in {stats.elapsed_seconds:.1f}s "
                  f"({stats.status_rows:,} status rows); waiting for "
                  "consumers to land everything...")

            elapsed, status_rows, info_rows = _wait_until_counts_stable(
                conn, start=start)
    finally:
        for proc in consumers:
            proc.terminate()
        for log in logs:
            log.close()

    _record(_make_result(
        "kafka", paths, elapsed, status_rows, info_rows,
        produced_status=stats.status_rows,
        produced_info=stats.info_rows,
        producer_seconds=round(stats.elapsed_seconds, 2),
    ))


def write_report() -> None:
    if not RESULTS_FILE.exists():
        sys.exit("No results yet — run a benchmark first.")

    with open(RESULTS_FILE, encoding="utf-8") as f:
        runs = [json.loads(line) for line in f if line.strip()]

    lines = [
        "# Ingestion Benchmark — Direct COPY vs. Kafka Streaming",
        "",
        "Both paths land the same rows in the same PostgreSQL bronze tables.",
        "`direct` = CSV -> server-side COPY. `kafka` = CSV -> producer -> Kafka "
        "-> consumers -> COPY. Times are end-to-end, including deduplication "
        "(`ON CONFLICT DO NOTHING`).",
        "",
        "| When (UTC) | Mode | Input | Rows landed | Seconds | Rows/s |",
        "|---|---|---|---:|---:|---:|",
    ]
    for r in runs:
        input_label = (r["input"][0] if len(r["input"]) == 1
                       else f"{len(r['input'])} files")
        lines.append(
            f"| {r['timestamp']} | {r['mode']} | {input_label} "
            f"| {r['rows_total']:,} | {r['elapsed_seconds']} "
            f"| {r['rows_per_second']:,} |"
        )

    best = {}
    for r in runs:
        if r["rows_per_second"] is not None:
            current = best.get(r["mode"])
            if current is None or r["rows_per_second"] > current:
                best[r["mode"]] = r["rows_per_second"]
    if {"direct", "kafka"} <= best.keys():
        ratio = best["direct"] / best["kafka"]
        lines += [
            "",
            f"**Best direct:** {best['direct']:,} rows/s · "
            f"**Best kafka:** {best['kafka']:,} rows/s · "
            f"direct is **{ratio:.1f}×** faster end-to-end.",
            "",
            "The trade-off: Kafka adds serialization, network hops, and "
            "consumer-side batching, but provides decoupling, replayability, "
            "back-pressure, and multiple independent consumers — none of "
            "which the direct path offers.",
        ]

    RESULTS_DIR.mkdir(exist_ok=True)
    REPORT_FILE.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"Report written to {REPORT_FILE.relative_to(PROJECT_ROOT)}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)

    for name in ("direct", "kafka"):
        p = sub.add_parser(name)
        scope = p.add_mutually_exclusive_group()
        scope.add_argument("--files", type=int,
                           help="ingest the first N raw CSV files")
        scope.add_argument("--max-rows", type=int,
                           help="ingest a sample capped at N rows")

    sub.add_parser("report")
    args = parser.parse_args()

    if args.command == "report":
        write_report()
        return

    paths = _resolve_input(args.files, args.max_rows)
    print(f"Benchmarking '{args.command}' over: {', '.join(p.name for p in paths)}")
    if args.command == "direct":
        run_direct(paths)
    else:
        run_kafka(paths)


if __name__ == "__main__":
    main()
