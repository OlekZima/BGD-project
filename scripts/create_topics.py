import subprocess

KAFKA_CONTAINER = "bgd_kafka"

TOPICS = [
    ("citibike-status", 8),
    ("citibike-info", 8),
]

def create_topic(name, partitions):
    cmd = [
        "docker", "exec", "-i", KAFKA_CONTAINER,
        "/opt/kafka/bin/kafka-topics.sh",
        "--create",
        "--if-not-exists",
        "--topic", name,
        "--bootstrap-server", "localhost:9092",
        "--partitions", str(partitions),
        "--replication-factor", "1"
    ]

    subprocess.run(cmd, check=True)
    print(f"Created topic: {name} ({partitions} partitions)")


if __name__ == "__main__":
    for name, partitions in TOPICS:
        create_topic(name, partitions)

    print("All topics ready")