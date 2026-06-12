"""Generic Kafka -> PostgreSQL consumer.

Batches messages in memory, lands them in an UNLOGGED staging table via COPY,
then projects them into the target bronze table with a single INSERT ... ON
CONFLICT DO NOTHING, so replays and duplicate deliveries are harmless.
"""

import json
import signal
import sys
import time

import psycopg
from kafka import KafkaConsumer

from settings import (
    KAFKA_API_VERSION,
    KAFKA_BOOTSTRAP_SERVERS,
    db_connection_kwargs,
)

FLUSH_INTERVAL_SECONDS = 5
FETCH_MAX_BYTES = 10 * 1024 * 1024
MAX_PARTITION_FETCH_BYTES = 5 * 1024 * 1024
POLL_TIMEOUT_MS = 1000


class CopyConsumer:
    """Consumes one topic and lands batches into PostgreSQL via COPY."""

    def __init__(
        self,
        topic: str,
        group_id: str,
        staging_table: str,
        staging_ddl: str,
        columns: tuple[str, ...],
        insert_sql: str,
        batch_size: int,
    ):
        self._topic = topic
        self._group_id = group_id
        self._staging_table = staging_table
        self._staging_ddl = staging_ddl
        self._columns = columns
        self._insert_sql = insert_sql
        self._batch_size = batch_size
        self._buffer: list[dict] = []
        self._copy_sql = (
            f"COPY {staging_table} ({', '.join(columns)}) FROM STDIN"
        )

    def _connect_kafka(self) -> KafkaConsumer:
        return KafkaConsumer(
            self._topic,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            api_version=KAFKA_API_VERSION,
            fetch_max_bytes=FETCH_MAX_BYTES,
            max_partition_fetch_bytes=MAX_PARTITION_FETCH_BYTES,
            group_id=self._group_id,
            enable_auto_commit=False,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        )

    def _connect_db(self) -> psycopg.Connection:
        conn = psycopg.connect(**db_connection_kwargs())
        conn.execute("SET synchronous_commit TO OFF")
        with conn.cursor() as cur:
            cur.execute(self._staging_ddl)
        conn.commit()
        return conn

    def _flush(self, conn: psycopg.Connection) -> None:
        if not self._buffer:
            return
        with conn.cursor() as cur:
            cur.execute(f"TRUNCATE {self._staging_table}")
            with cur.copy(self._copy_sql) as copy:
                for row in self._buffer:
                    copy.write_row(tuple(row.get(c) for c in self._columns))
            cur.execute(self._insert_sql)
        conn.commit()
        self._buffer.clear()

    def run(self) -> None:
        print(f"[{self._group_id}] connecting to Kafka at {KAFKA_BOOTSTRAP_SERVERS}...",
              flush=True)
        consumer = self._connect_kafka()
        conn = self._connect_db()
        last_flush = time.monotonic()

        def shutdown(*_args):
            if self._buffer:
                self._flush(conn)
                consumer.commit()
            sys.exit(0)

        signal.signal(signal.SIGINT, shutdown)
        signal.signal(signal.SIGTERM, shutdown)

        print(f"[{self._group_id}] consuming '{self._topic}'", flush=True)
        while True:
            records = consumer.poll(timeout_ms=POLL_TIMEOUT_MS)
            for messages in records.values():
                for msg in messages:
                    self._buffer.extend(msg.value)

            now = time.monotonic()
            flush_due = self._buffer and now - last_flush > FLUSH_INTERVAL_SECONDS
            if len(self._buffer) >= self._batch_size or flush_due:
                rows = len(self._buffer)
                self._flush(conn)
                consumer.commit()
                last_flush = now
                print(f"[{self._group_id}] flushed {rows:,} rows", flush=True)
