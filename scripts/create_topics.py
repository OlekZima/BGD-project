#!/usr/bin/env python3
"""Creates the Kafka topics inside the broker container (idempotent)."""

import subprocess

from settings import KAFKA_CONTAINER, TOPIC_INFO, TOPIC_STATUS

TOPICS = [
    (TOPIC_STATUS, 8),
    (TOPIC_INFO, 8),
]


def create_topic(name: str, partitions: int) -> None:
    cmd = [
        "docker", "exec", "-i", KAFKA_CONTAINER,
        "/opt/kafka/bin/kafka-topics.sh",
        "--create",
        "--if-not-exists",
        "--topic", name,
        "--bootstrap-server", "localhost:9092",
        "--partitions", str(partitions),
        "--replication-factor", "1",
    ]
    subprocess.run(cmd, check=True)
    print(f"Topic ready: {name} ({partitions} partitions)")


if __name__ == "__main__":
    for topic_name, topic_partitions in TOPICS:
        create_topic(topic_name, topic_partitions)
