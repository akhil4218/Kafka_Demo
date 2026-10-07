"""Read sample order events from Kafka and show streaming latency."""

import argparse
import json
import time

from kafka import KafkaConsumer

from kafka_demo import BOOTSTRAP_SERVERS, TOPIC, ensure_topic


def main() -> None:
    parser = argparse.ArgumentParser(description="Read sample events from Kafka.")
    parser.add_argument("--count", type=int, default=5, help="number of events to read")
    parser.add_argument("--group", default="shipping-demo", help="Kafka consumer group name")
    parser.add_argument("--from-beginning", action="store_true", help="read stored events when this group is new")
    args = parser.parse_args()

    ensure_topic()
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=args.group,
        auto_offset_reset="earliest" if args.from_beginning else "latest",
        enable_auto_commit=True,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )
    print("\n=== KAFKA CONSUMER: Kitchen Service ===")
    print(f"Consumer group: '{args.group}'")
    print(f"Listening for {args.count} order event(s) on topic '{TOPIC}'...\n")
    received = 0
    try:
        for message in consumer:
            event = message.value
            latency_ms = (time.time() - event["produced_at"]) * 1000
            order_id = event.get("order_id", f"legacy order #{event.get('number', '?')}")
            customer = event.get("customer", "Unknown customer")
            item = event.get("item", "Unknown item")
            amount = event.get("amount", "?")
            print(
                f"[CONSUMER] Order received: {order_id} | {customer} ordered {item} | Rs. {amount}\n"
                f"           Kafka details -> partition {message.partition}, offset {message.offset}, "
                f"delivery delay {latency_ms:.1f} ms\n"
            )
            received += 1
            if received >= args.count:
                break
    finally:
        consumer.close()
        print("=== CONSUMER FINISHED ===")


if __name__ == "__main__":
    main()
