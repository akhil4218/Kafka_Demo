"""Compare direct work with a Kafka-like streaming queue; no broker required."""

import argparse
import queue
import threading
import time


def direct_work(count: int, work_seconds: float) -> tuple[float, float]:
    start = time.perf_counter()
    for _ in range(count):
        time.sleep(work_seconds)  # Sender is blocked by the receiving service.
    done = time.perf_counter()
    return done - start, done - start


def streamed_work(count: int, work_seconds: float, workers: int) -> tuple[float, float]:
    events: queue.Queue[int | None] = queue.Queue()

    def consumer() -> None:
        while True:
            event = events.get()
            if event is None:
                events.task_done()
                return
            time.sleep(work_seconds)  # Pretend this is a separate slow service.
            events.task_done()

    threads = [threading.Thread(target=consumer, daemon=True) for _ in range(workers)]
    for thread in threads:
        thread.start()

    start = time.perf_counter()
    for event_number in range(count):
        events.put(event_number)  # Like publishing an event: quick, not slow work.
    sender_done = time.perf_counter()
    for _ in threads:
        events.put(None)
    events.join()
    all_done = time.perf_counter()
    return sender_done - start, all_done - start


def main() -> None:
    parser = argparse.ArgumentParser(description="Show why queues help streaming systems.")
    parser.add_argument("--count", type=int, default=12)
    parser.add_argument("--work-seconds", type=float, default=0.2)
    parser.add_argument("--workers", type=int, default=3)
    args = parser.parse_args()
    if args.count < 1 or args.work_seconds < 0 or args.workers < 1:
        parser.error("count and workers must be at least 1; work-seconds cannot be negative")

    direct_sender, direct_done = direct_work(args.count, args.work_seconds)
    stream_sender, stream_done = streamed_work(args.count, args.work_seconds, args.workers)
    print("\n=== WHY STREAMING HELPS: TIMING COMPARISON ===")
    print(f"Without Kafka (direct calls):")
    print(f"  The order screen waits {direct_sender:.3f}s; all orders finish in {direct_done:.3f}s.")
    print(f"With a stream and {args.workers} workers:")
    print(f"  The order screen is free in {stream_sender:.3f}s; all orders finish in {stream_done:.3f}s.")
    print(f"\nTime the sender did not have to wait: {direct_sender - stream_sender:.3f}s")
    print(f"Total processing time saved by parallel workers: {direct_done - stream_done:.3f}s")
    print("Kafka is the durable shared stream between the order screen and the workers.")


if __name__ == "__main__":
    main()
