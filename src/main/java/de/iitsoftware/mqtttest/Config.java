package de.iitsoftware.mqtttest;

/**
 * Parsed command-line configuration for the MQTT throughput test.
 *
 * <p>Simple hand-rolled {@code --key value} parser so the tool stays dependency-free
 * beyond the MQTT client itself.</p>
 */
public final class Config {

    /** Minimum payload size: 8 bytes sequence number + 8 bytes send timestamp. */
    public static final int HEADER_SIZE = 16;

    /** Test mode. */
    public enum Mode {
        /** Subscriber consumes while the publisher sends (end-to-end pipeline). */
        PARALLEL,
        /** Send all messages with the consumer offline, then reconnect and drain the broker queue. */
        SEND_DRAIN,
        /** Run the full matrix: send-drain (QoS 1,2) then parallel (QoS 0,1,2), then print a table. */
        ALL
    }

    public final String broker;
    public final String topic;
    public final int qos;
    public final int size;
    public final long count;
    public final int intervalSeconds;
    public final Mode mode;
    public final String username;   // may be null
    public final String password;   // may be null
    public final String clientPrefix;
    public final long throttleEvery;
    public final long throttleDelayMs;
    public final int receiveIdleTimeoutSeconds;
    public final boolean skipDrainQos0;

    private Config(String broker, String topic, int qos, int size, long count,
                   int intervalSeconds, Mode mode, String username, String password, String clientPrefix,
                   long throttleEvery, long throttleDelayMs, int receiveIdleTimeoutSeconds,
                   boolean skipDrainQos0) {
        this.broker = broker;
        this.topic = topic;
        this.qos = qos;
        this.size = size;
        this.count = count;
        this.intervalSeconds = intervalSeconds;
        this.mode = mode;
        this.username = username;
        this.password = password;
        this.clientPrefix = clientPrefix;
        this.throttleEvery = throttleEvery;
        this.throttleDelayMs = throttleDelayMs;
        this.receiveIdleTimeoutSeconds = receiveIdleTimeoutSeconds;
        this.skipDrainQos0 = skipDrainQos0;
    }

    /** Create a copy with a different mode, QoS and topic (used by the {@code all} matrix runner). */
    public Config derive(Mode mode, int qos, String topic) {
        return new Config(broker, topic, qos, size, count, intervalSeconds, mode, username, password,
                clientPrefix, throttleEvery, throttleDelayMs, receiveIdleTimeoutSeconds, skipDrainQos0);
    }

    public static String usage() {
        return """
                MQTT 3.1.1 throughput test client.

                Publishes N messages of a fixed size to a topic at a given QoS, subscribes to the
                same topic at the same QoS, and reports send/receive throughput.

                Usage:
                  java -jar mqtt_test-1.0-SNAPSHOT.jar [options]

                Options:
                  --broker    URL    MQTT broker URL              (default tcp://localhost:1883)
                  --topic     NAME   topic to publish/subscribe   (default perf/test)
                  --qos       0|1|2  QoS for publish and subscribe (default 1)
                  --size      BYTES  payload size, min 16         (default 256)
                  --count     N      number of messages           (default 200000)
                  --interval  SECS   throughput report interval    (default 5)
                  --mode      MODE   parallel | send-drain | all  (default all)
                  --throttle-every N sleep after every N published messages (default 0, disabled)
                  --throttle-delay MS sleep duration in milliseconds       (default 0)
                  --receive-idle-timeout SECS
                                       stop waiting after no receive progress (default auto)
                  --skip-drain-qos0    skip send-drain QoS 0 in all/matrix mode
                  --username  USER   broker username              (optional)
                  --password  PASS   broker password              (optional)
                  --client    PREFIX client-id prefix             (default mqtt-perf)
                  --help             print this help and exit

                Modes:
                  parallel    Subscriber consumes while the publisher sends. Measures the
                              end-to-end publisher -> broker -> subscriber pipeline.
                  send-drain  Send all messages with the consumer offline (broker queues them
                              in a durable session), then reconnect and drain. Reports send and
                              drain throughput separately. Requires QoS >= 1, and the broker must
                              be able to queue all N messages.
                  all         Run the full matrix and print a comparison table: send-drain at
                              QoS 0, 1 and 2, then parallel at QoS 0, 1 and 2.
                """;
    }

    /**
     * @return parsed config, or {@code null} if {@code --help} was requested (caller should exit 0).
     * @throws IllegalArgumentException on malformed input.
     */
    public static Config parse(String[] args) {
        String broker = "tcp://localhost:1883";
        String topic = "perf/test";
        int qos = 1;
        int size = 256;
        long count = 200_000L;
        int interval = 5;
        Mode mode = Mode.ALL;
        String username = null;
        String password = null;
        String clientPrefix = "mqtt-perf";
        long throttleEvery = 0;
        long throttleDelayMs = 0;
        int receiveIdleTimeoutSeconds = 0;
        boolean skipDrainQos0 = false;

        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            switch (arg) {
                case "--help", "-h" -> { return null; }
                case "--broker"   -> broker = value(args, ++i, arg);
                case "--topic"    -> topic = value(args, ++i, arg);
                case "--qos"      -> qos = parseInt(value(args, ++i, arg), arg);
                case "--size"     -> size = parseInt(value(args, ++i, arg), arg);
                case "--count"    -> count = parseLong(value(args, ++i, arg), arg);
                case "--interval" -> interval = parseInt(value(args, ++i, arg), arg);
                case "--mode"     -> mode = parseMode(value(args, ++i, arg));
                case "--throttle-every" -> throttleEvery = parseLong(value(args, ++i, arg), arg);
                case "--throttle-delay" -> throttleDelayMs = parseLong(value(args, ++i, arg), arg);
                case "--receive-idle-timeout" -> receiveIdleTimeoutSeconds = parseInt(value(args, ++i, arg), arg);
                case "--skip-drain-qos0" -> skipDrainQos0 = true;
                case "--username" -> username = value(args, ++i, arg);
                case "--password" -> password = value(args, ++i, arg);
                case "--client"   -> clientPrefix = value(args, ++i, arg);
                default -> throw new IllegalArgumentException("Unknown option: " + arg);
            }
        }

        if (qos < 0 || qos > 2) {
            throw new IllegalArgumentException("--qos must be 0, 1 or 2 (got " + qos + ")");
        }
        if (size < HEADER_SIZE) {
            throw new IllegalArgumentException("--size must be >= " + HEADER_SIZE + " (got " + size + ")");
        }
        if (count <= 0) {
            throw new IllegalArgumentException("--count must be > 0 (got " + count + ")");
        }
        if (interval <= 0) {
            throw new IllegalArgumentException("--interval must be > 0 (got " + interval + ")");
        }
        if (throttleEvery < 0) {
            throw new IllegalArgumentException("--throttle-every must be >= 0 (got " + throttleEvery + ")");
        }
        if (throttleDelayMs < 0) {
            throw new IllegalArgumentException("--throttle-delay must be >= 0 (got " + throttleDelayMs + ")");
        }
        if ((throttleEvery == 0) != (throttleDelayMs == 0)) {
            throw new IllegalArgumentException("--throttle-every and --throttle-delay must both be > 0, "
                    + "or both be 0 to disable throttling");
        }
        if (receiveIdleTimeoutSeconds < 0) {
            throw new IllegalArgumentException("--receive-idle-timeout must be >= 0 (got "
                    + receiveIdleTimeoutSeconds + ")");
        }
        if (mode == Mode.SEND_DRAIN && qos == 0) {
            System.err.println("Warning: send-drain at QoS 0 relies on the broker storing QoS 0 messages "
                    + "for an offline durable session, which the MQTT spec does not require. "
                    + "Expect heavy loss unless the broker explicitly supports it.");
        }

        return new Config(broker, topic, qos, size, count, interval, mode, username, password, clientPrefix,
                throttleEvery, throttleDelayMs, receiveIdleTimeoutSeconds, skipDrainQos0);
    }

    private static Mode parseMode(String v) {
        return switch (v.toLowerCase()) {
            case "parallel" -> Mode.PARALLEL;
            case "send-drain", "senddrain", "drain" -> Mode.SEND_DRAIN;
            case "all", "matrix" -> Mode.ALL;
            default -> throw new IllegalArgumentException(
                    "Invalid --mode: " + v + " (expected 'parallel', 'send-drain' or 'all')");
        };
    }

    private static String value(String[] args, int i, String opt) {
        if (i >= args.length) {
            throw new IllegalArgumentException("Missing value for " + opt);
        }
        return args[i];
    }

    private static int parseInt(String v, String opt) {
        try {
            return Integer.parseInt(v);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid integer for " + opt + ": " + v);
        }
    }

    private static long parseLong(String v, String opt) {
        try {
            return Long.parseLong(v);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid integer for " + opt + ": " + v);
        }
    }

    @Override
    public String toString() {
        return "broker=" + broker + ", topic=" + topic + ", qos=" + qos
                + ", size=" + size + "B, count=" + count
                + ", interval=" + intervalSeconds + "s"
                + (throttleEvery > 0 ? ", throttleEvery=" + throttleEvery
                        + ", throttleDelay=" + throttleDelayMs + "ms" : "")
                + (receiveIdleTimeoutSeconds > 0 ? ", receiveIdleTimeout="
                        + receiveIdleTimeoutSeconds + "s" : "")
                + (skipDrainQos0 ? ", skipDrainQos0=true" : "")
                + ", mode=" + switch (mode) {
                    case SEND_DRAIN -> "send-drain";
                    case PARALLEL -> "parallel";
                    case ALL -> "all";
                }
                + (username != null ? ", username=" + username : "");
    }
}
