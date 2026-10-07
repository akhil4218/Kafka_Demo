"""Publish sample order events to Kafka."""

import argparse
import json
import time

from kafka import KafkaProducer

from kafka_demo import BOOTSTRAP_SERVERS, TOPIC, ensure_topic


def main() -> None:
    parser = argparse.ArgumentParser(description="Send sample events to Kafka.")
    parser.add_argument("--count", type=int, default=5, help="number of events to send")
    parser.add_argument("--delay", type=float, default=1.0, help="seconds between events")
    args = parser.parse_args()

    ensure_topic()
    sample_orders = [
        {"customer": "Asha", "item": "Cappuccino", "amount": 180},
        {"customer": "Ravi", "item": "Veg Sandwich", "amount": 140},
        {"customer": "Meera", "item": "Cold Coffee", "amount": 160},
        {"customer": "Arjun", "item": "Chocolate Muffin", "amount": 110},
        {"customer": "Neha", "item": "Masala Chai", "amount": 60},
    ]
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda event: json.dumps(event).encode("utf-8"),
    )
    try:
        print("\n=== KAFKA PRODUCER: Coffee Shop Orders ===")
        print(f"Sending {args.count} order event(s) to topic '{TOPIC}'...\n")
        for number in range(1, args.count + 1):
            order = sample_orders[(number - 1) % len(sample_orders)]
            event = {
                "event_type": "order_created",
                "order_id": f"COFFEE-{1000 + number}",
                "customer": order["customer"],
                "item": order["item"],
                "amount": order["amount"],
                "produced_at": time.time(),
            }
            metadata = producer.send(TOPIC, value=event).get(timeout=10)
            print(
                f"[PRODUCER] New order: {event['order_id']} | {event['customer']} ordered "
                f"{event['item']} | Rs. {event['amount']}\n"
                f"           Saved in Kafka -> partition {metadata.partition}, offset {metadata.offset}\n"
            )
            time.sleep(args.delay)
    finally:
        producer.flush()
        producer.close()
        print("=== PRODUCER FINISHED ===")


if __name__ == "__main__":
    main()
