# mqtt_test — MQTT 3.1.1 throughput test client

A small Java tool that benchmarks MQTT throughput. It publishes N fixed-size messages to a topic at
a configurable QoS, receives them back from the same topic, and reports send/receive throughput —
periodic updates while running plus a final summary. It can run a single configuration or a full
matrix of modes × QoS levels and print a comparison table.

- **Protocol:** MQTT 3.1.1
- **Client library:** [Eclipse Paho Java Client v3](https://github.com/eclipse/paho.mqtt.java)
  (`org.eclipse.paho.client.mqttv3`) — the reference MQTT 3.1.1 client. The async client is used so
  QoS 1/2 publishes pipeline (bounded in-flight window) instead of blocking per message.
- **Build:** Maven, Java 21.

## Requirements

- JDK 21+
- Maven 3.9+
- A running MQTT broker reachable at the configured URL (default `tcp://localhost:1883`).
  Any MQTT 3.1.1 broker works (SwiftMQ, Mosquitto, HiveMQ, …).

## Build

```bash
mvn clean package
```

This produces a runnable fat jar at `target/mqtt_test-1.0-SNAPSHOT.jar`.

## Quick start

```bash
# Default: full matrix (send-drain QoS 0/1/2, then parallel QoS 0/1/2), 200,000 msgs of 256 B
java -jar target/mqtt_test-1.0-SNAPSHOT.jar

# A single end-to-end run at QoS 1
java -jar target/mqtt_test-1.0-SNAPSHOT.jar --mode parallel --qos 1 --count 100000

# Point at a remote broker
java -jar target/mqtt_test-1.0-SNAPSHOT.jar --broker tcp://broker.example.com:1883
```

## Options

| Option | Values | Default | Description |
|---|---|---|---|
| `--broker` | URL | `tcp://localhost:1883` | MQTT broker URL |
| `--topic` | name | `perf/test` | Topic to publish/subscribe |
| `--qos` | `0` \| `1` \| `2` | `1` | QoS for both publish and subscribe |
| `--size` | bytes (≥ 16) | `256` | Payload size |
| `--count` | N | `200000` | Number of messages |
| `--interval` | seconds | `5` | Throughput report interval |
| `--mode` | `parallel` \| `send-drain` \| `all` | `all` | Test mode (see below) |
| `--username` | user | — | Broker username (optional) |
| `--password` | pass | — | Broker password (optional) |
| `--client` | prefix | `mqtt-perf` | Client-id prefix |
| `--help` | | | Print usage and exit |

The first 16 bytes of each payload carry an 8-byte sequence number and an 8-byte send timestamp, so
the tool can detect loss and measure end-to-end latency. Hence the minimum payload size of 16 bytes.

## Modes

### `parallel`
The subscriber consumes while the publisher sends. Measures the end-to-end
**publisher → broker → subscriber** pipeline. This is the throughput you get when producer and
consumer run at the same time. Latency (min/avg/max) is reported because messages flow live.

### `send-drain`
All messages are published **with the consumer offline**, so the broker queues them in a durable
session (`cleanSession=false`); then the consumer reconnects and drains the queue. This separates
two distinct numbers:

- **Send rate** — how fast the broker accepts published messages (consumer not involved).
- **Drain rate** — how fast a reconnecting consumer pulls the backlog out.

Requirements/notes:
- Needs **QoS ≥ 1** in the general case — the MQTT spec does not require brokers to store QoS 0
  messages for offline subscribers. QoS 0 is allowed but prints a warning; whether it works depends
  on the broker (e.g. SwiftMQ does store QoS 0 for durable sessions).
- The broker must be able to queue all N messages for the offline session. If its queue limit is
  smaller, the excess is dropped and shown in the **Lost** column.
- Latency is not reported in this mode (it would just measure how long messages sat in the queue).

### `all`
Runs the full matrix and prints a comparison table: **send-drain at QoS 0, 1, 2**, then
**parallel at QoS 0, 1, 2**. Each run uses an isolated topic (`<topic>/sd|par/q<n>`) and fresh
client IDs, and send-drain clears its durable session afterward, so runs don't contaminate each
other.

## Output

While running, a line is printed every `--interval` seconds, e.g.:

```
[   4.0s] recv 36,698 msgs | window: 10,287 msg/s  2.63 MB/s | total: 9,168 msg/s  2.35 MB/s
```

- `window:` rate over the last interval. `total:` cumulative rate for the current phase.
- The verb is `sent` during a send phase and `recv` during a receive/drain phase.

Each run ends with a summary (duration, messages sent/received, lost count, throughput, and — for
`parallel` — end-to-end latency). In `all` mode a final table is printed:

```
=================================== Throughput summary ===================================
  Broker: tcp://localhost:1883   payload: 256 B   messages: 200,000 per run
-----------------------------------------------------------------------------------------
  Mode         QoS      Send msg/s    Send MB/s      Recv msg/s    Recv MB/s      Lost
  ---------------------------------------------------------------------------------------
  send-drain     0         305,899         78.3          27,371          7.0         0
  send-drain     1          41,596         10.6           3,637          0.9         0
  send-drain     2          41,933         10.7             678          0.2         0
  parallel       0               -            -         128,230         32.8         0
  parallel       1               -            -           3,036          0.8         0
  parallel       2               -            -             586          0.1         0
=========================================================================================
```

(Numbers above are illustrative — single publisher/subscriber, loopback to a SwiftMQ broker. They
show per-connection ceilings and the relative cost of QoS, not a broker's aggregate capacity.)

## How it works

- Two async Paho clients (one publisher, one subscriber) connect to the broker; messages loop
  publisher → broker → subscriber.
- Publishing uses a bounded in-flight window (a semaphore released on `deliveryComplete`) so QoS 1/2
  pipelines without unbounded memory growth and applies natural backpressure.
- The subscriber reads the payload header to count messages, detect loss, and compute latency.
- A run completes when all N messages are received, or after an idle timeout following the last
  message (which surfaces QoS 0 loss instead of hanging).

## Project layout

```
src/main/java/de/iitsoftware/mqtttest/
  Main.java     entry point, mode dispatch, client wiring, matrix runner, table
  Config.java   CLI parsing, defaults, validation, usage text
  Stats.java    thread-safe counters, per-phase reporter, summaries
  Result.java   one matrix row (throughput per mode/QoS)
```

## License

[MIT](LICENSE) © IIT Software GmbH
