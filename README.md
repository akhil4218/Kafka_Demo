# Kafka in simple words: a runnable demo

Apache Kafka is a system for moving events (small messages about something that happened) from one application to other applications.

Think of a food-order screen in a restaurant. The order screen puts each order on a shared queue. The kitchen, billing system, delivery tracker, and analytics system can each read the orders at their own speed. Kafka is that reliable, scalable queue for software.

## Why Kafka is needed

Without Kafka, an application often calls another application directly. If the receiving application is slow or unavailable, the sender must wait or may fail. Adding a new receiver usually means changing the sender too.

With Kafka, the producer writes an event once to a **topic**. Kafka keeps the event for a configured time, and one or many consumers read it independently. The producer and consumers are separated: they do not need to be online at exactly the same time.

| Without Kafka | With Kafka |
| --- | --- |
| App A directly calls App B. | App A publishes to a Kafka topic. |
| A waits for B and is affected when B is down. | A can continue after Kafka accepts the event; B can catch up later. |
| Every new receiver adds another connection to A. | New consumers subscribe to the topic without changing A. |
| Scaling and replaying old data are harder. | Consumers can be put in groups for parallel work and can replay retained events. |

Kafka is useful for order events, click tracking, IoT readings, logs, payments, and any steady flow of data that several systems need.

## Main Kafka words

- **Event/message**: one record, such as `order_created`.
- **Producer**: the program that writes events.
- **Topic**: the named stream of related events (`demo-events` in this project).
- **Broker**: a Kafka server that stores topics.
- **Consumer**: the program that reads and acts on events.
- **Consumer group**: consumers cooperating on a topic; Kafka divides partitions between them.
- **Offset**: a consumer's position in a topic, so it can resume after a restart.

## What this project demonstrates

`producer.py` creates timestamped events and sends them to `demo-events`.

`consumer.py` reads the events one by one and prints their end-to-end delay. Start it before the producer to watch a live stream. Kafka stores events, so a consumer can also start later and read them from the beginning.

`stream_simulation.py` needs no broker. It compares a slow direct workflow with a queue and three workers. It shows two practical benefits of streaming: the sender is free quickly, and independent work can happen in parallel.

## Run it — in this order

Prerequisites: Python 3.10+ and Docker Desktop.

### 1. Open PowerShell in this project folder

```powershell
cd C:\Projects\Kafka
```

### 2. Install the Python package

```powershell
py -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

### 3. Start Kafka

```powershell
docker compose up -d
docker compose ps
```

You should see `kafka-demo` with status `Up` and port `9092`.

### 4. Open a second PowerShell window and start the consumer first

```powershell
cd C:\Projects\Kafka
.\.venv\Scripts\Activate.ps1
python consumer.py --count 5 --group coffee-kitchen-live
```

Leave this window open. It will display `Listening for 5 order event(s)...`.
If you repeat the demo later, change the final number in the group name (for example, `coffee-kitchen-live-2`). A new consumer group starts cleanly and waits for the new orders.

### 5. Return to the first PowerShell window and send coffee-shop orders

```powershell
python producer.py --count 5 --delay 1
```

The producer will show messages such as:

```text
[PRODUCER] New order: COFFEE-1001 | Asha ordered Cappuccino | Rs. 180
           Saved in Kafka -> partition 1, offset 0
```

The consumer window will show the same order arrive in real time:

```text
[CONSUMER] Order received: COFFEE-1001 | Asha ordered Cappuccino | Rs. 180
           Kafka details -> partition 1, offset 0, delivery delay 12.4 ms
```

### 6. See the timing benefit without needing Kafka

```powershell
python stream_simulation.py --count 12 --work-seconds 0.2 --workers 3
```

### 7. Stop Kafka when finished

```powershell
docker compose down
```

## Understanding the time result

The simulation reports two measurements: when the sender becomes free, and when all work is complete. With the defaults, direct processing takes about 2.4 seconds. The streamed version lets the sender finish immediately and three workers finish the work in about 0.8 seconds. Your numbers will vary.

Kafka does **not** magically make one piece of work faster. It saves time when it removes waiting between systems, allows consumers to scale, and lets downstream services recover and catch up instead of blocking the sender. The real Kafka scripts print each event's latency; the simulation makes the timing comparison safe to run even when Docker is not available.

## Useful variations

Produce faster:

```powershell
python producer.py --count 20 --delay 0.1
```

Start another consumer group (it gets its own copy of the stream):

```powershell
python consumer.py --count 5 --group analytics-demo --from-beginning
```

For a clean local Kafka reset, use `docker compose down -v`. This removes the broker's stored demo messages.
