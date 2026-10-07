"""Shared settings and topic setup for the Kafka example."""

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "demo-events"


def ensure_topic() -> None:
    """Create the demo topic once, if it is not already present."""
    admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)
    try:
        admin.create_topics([NewTopic(name=TOPIC, num_partitions=3, replication_factor=1)])
        print(f"Created topic '{TOPIC}' with 3 partitions.")
    except TopicAlreadyExistsError:
        pass
    finally:
        admin.close()
