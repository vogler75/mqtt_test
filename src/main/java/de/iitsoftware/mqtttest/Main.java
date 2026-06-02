package de.iitsoftware.mqtttest;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.IMqttToken;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * MQTT 3.1.1 throughput test client.
 *
 * <p>Publishes N fixed-size messages to a topic at a given QoS and measures throughput in one of
 * three modes:</p>
 * <ul>
 *   <li><b>parallel</b> — the subscriber consumes while the publisher sends; measures the
 *       end-to-end publisher → broker → subscriber pipeline.</li>
 *   <li><b>send-drain</b> — all messages are sent with the consumer offline (the broker queues them
 *       in a durable session), then the consumer reconnects and drains; reports send and drain
 *       throughput separately.</li>
 *   <li><b>all</b> — runs the full matrix (send-drain at QoS 1,2 then parallel at QoS 0,1,2) and
 *       prints a comparison table.</li>
 * </ul>
 */
public class Main {

    /** Max in-flight publishes (bounds memory and provides backpressure for QoS 1/2). */
    private static final int MAX_INFLIGHT = 1000;
    private static final long CONTROL_COMPLETION_TIMEOUT_MS = 30_000;
    private static final long DISCONNECT_QUIESCE_TIMEOUT_MS = 1_000;
    private static final long DISCONNECT_COMPLETION_TIMEOUT_MS = 5_000;

    public static void main(String[] args) throws Exception {
        Config config;
        try {
            config = Config.parse(args);
        } catch (IllegalArgumentException e) {
            System.err.println("Error: " + e.getMessage());
            System.err.println();
            System.err.println(Config.usage());
            System.exit(2);
            return;
        }
        if (config == null) { // --help
            System.out.println(Config.usage());
            return;
        }

        System.out.println("Config: " + config);
        int exitCode = 0;
        try {
            new Main().run(config);
        } catch (Exception e) {
            exitCode = 1;
            e.printStackTrace(System.err);
        } finally {
            System.exit(exitCode);
        }
    }

    private void run(Config config) throws Exception {
        if (config.mode == Config.Mode.ALL) {
            runAll(config);
        } else {
            runSingle(config, config.mode, config.qos);
        }
    }

    /** Run the full matrix: send-drain then parallel, then print a table. */
    private void runAll(Config config) throws Exception {
        List<Result> results = new ArrayList<>();

        int[] sendDrainQos = config.skipDrainQos0 ? new int[]{1, 2} : new int[]{0, 1, 2};
        for (int qos : sendDrainQos) {
            banner("send-drain", qos);
            results.add(runSingle(config, Config.Mode.SEND_DRAIN, qos));
        }
        for (int qos : new int[]{0, 1, 2}) {
            banner("parallel", qos);
            results.add(runSingle(config, Config.Mode.PARALLEL, qos));
        }

        printTable(results, config);
    }

    private void banner(String mode, int qos) {
        System.out.println();
        System.out.println("########################################################");
        System.out.println("##  Running mode=" + mode + " QoS=" + qos);
        System.out.println("########################################################");
    }

    /** Set up fresh clients for one (mode, QoS) run on an isolated topic, execute it, and tear down. */
    private Result runSingle(Config base, Config.Mode mode, int qos) throws Exception {
        String tag = (mode == Config.Mode.SEND_DRAIN) ? "sd" : "par";
        Config config = base.derive(mode, qos, base.topic + "/" + tag + "/q" + qos);

        Stats stats = new Stats();
        String suffix = "-" + (System.nanoTime() % 100000);
        MqttAsyncClient subscriber =
                new MqttAsyncClient(config.broker, config.clientPrefix + "-sub" + suffix, new MemoryPersistence());
        MqttAsyncClient publisher =
                new MqttAsyncClient(config.broker, config.clientPrefix + "-pub" + suffix, new MemoryPersistence());

        int maxInflight = (int) Math.min(MAX_INFLIGHT, Math.max(10, config.count));
        Semaphore inflight = new Semaphore(maxInflight);

        ScheduledExecutorService reporter = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "reporter");
            t.setDaemon(true);
            return t;
        });

        subscriber.setManualAcks(true);
        subscriber.setCallback(receiveCallback(subscriber, stats));
        publisher.setCallback(publishCallback(inflight));

        try {
            return (mode == Config.Mode.SEND_DRAIN)
                    ? runSendDrain(config, subscriber, publisher, inflight, maxInflight, stats, reporter)
                    : runParallel(config, subscriber, publisher, inflight, maxInflight, stats, reporter);
        } finally {
            reporter.shutdownNow();
            disconnectAndClose(publisher);
            disconnectAndClose(subscriber);
        }
    }

    /** Subscriber consumes while the publisher sends. */
    private Result runParallel(Config config, MqttAsyncClient subscriber, MqttAsyncClient publisher,
                               Semaphore inflight, int maxInflight, Stats stats,
                               ScheduledExecutorService reporter) throws Exception {
        System.out.println("Connecting subscriber...");
        connectAndWait(subscriber, config, maxInflight, true, "parallel subscriber connect");
        System.out.println("Subscriber connected (CONNACK received).");

        System.out.println("Subscribing to '" + config.topic + "' at QoS " + config.qos + "...");
        subscribeAndWait(subscriber, config.topic, config.qos);
        System.out.println("Subscription acknowledged (SUBACK received).");

        System.out.println("Connecting publisher...");
        connectAndWait(publisher, config, maxInflight, true, "parallel publisher connect");
        System.out.println("Publisher connected.");

        stats.beginPhase(false); // track received
        ScheduledFuture<?> ticker = scheduleReporter(reporter, stats, config);

        System.out.println("Publishing " + String.format("%,d", config.count)
                + " messages of " + config.size + " bytes...");
        publish(config, publisher, inflight, stats);
        inflight.acquire(maxInflight); // drain remaining in-flight publishes
        System.out.println("All messages published; waiting for receiver to drain...");

        awaitReceive(config, stats);

        ticker.cancel(false);
        stats.report();
        double recvMsgRate = stats.phaseMsgRate();
        double recvMbRate = stats.phaseMbRate();
        stats.printParallelSummary(config.count);

        long lost = Math.max(0, config.count - stats.receivedMessages());
        return new Result("parallel", config.qos, -1, -1, recvMsgRate, recvMbRate,
                lost, stats.outOfOrder());
    }

    /** Send all messages with the consumer offline, then reconnect and drain the broker queue. */
    private Result runSendDrain(Config config, MqttAsyncClient subscriber, MqttAsyncClient publisher,
                                Semaphore inflight, int maxInflight, Stats stats,
                                ScheduledExecutorService reporter) throws Exception {
        System.out.println("[send-drain] Note: the broker must be able to queue all "
                + String.format("%,d", config.count) + " messages for an offline session. "
                + "Messages beyond the broker's queue limit are dropped and reported as lost.");

        // Establish a durable subscription, then go offline so the broker queues matching messages.
        System.out.println("[send-drain] Establishing durable subscription on '" + config.topic
                + "' at QoS " + config.qos + "...");
        connectAndWait(subscriber, config, maxInflight, false, "send-drain subscriber connect");
        System.out.println("[send-drain] Subscriber connected (CONNACK received).");
        subscribeAndWait(subscriber, config.topic, config.qos);
        System.out.println("[send-drain] Durable subscription acknowledged (SUBACK received).");
        System.out.println("[send-drain] Taking subscriber offline before publishing...");
        forceOffline(subscriber);
        System.out.println("[send-drain] Subscriber is offline; connecting publisher...");

        // --- Send phase ---
        connectAndWait(publisher, config, maxInflight, true, "send-drain publisher connect");
        System.out.println("[send-drain] Sending " + String.format("%,d", config.count)
                + " messages of " + config.size + " bytes...");
        stats.beginPhase(true); // track sent
        ScheduledFuture<?> sendTicker = scheduleReporter(reporter, stats, config);
        publish(config, publisher, inflight, stats);
        inflight.acquire(maxInflight); // all messages handed to the broker
        sendTicker.cancel(false);
        stats.report();
        double sendMsgRate = stats.phaseMsgRate();
        double sendMbRate = stats.phaseMbRate();
        stats.printSendSummary();
        disconnectAndWait(publisher);

        // --- Drain phase ---
        System.out.println();
        System.out.println("[send-drain] Reconnecting consumer to drain queued messages...");
        stats.beginPhase(false); // track received
        ScheduledFuture<?> drainTicker = scheduleReporter(reporter, stats, config);
        connectAndWait(subscriber, config, maxInflight, false, "send-drain subscriber reconnect"); // resume session
        awaitReceive(config, stats);
        drainTicker.cancel(false);
        stats.report();
        double recvMsgRate = stats.phaseMsgRate();
        double recvMbRate = stats.phaseMbRate();
        stats.printDrainSummary(config.count);

        long lost = Math.max(0, config.count - stats.receivedMessages());

        // Clear the durable session from the broker so it doesn't linger between matrix runs.
        clearDurableSession(subscriber, config, maxInflight);

        return new Result("send-drain", config.qos, sendMsgRate, sendMbRate, recvMsgRate, recvMbRate,
                lost, stats.outOfOrder());
    }

    /** Reconnect with a clean session and disconnect, telling the broker to discard the durable session. */
    private void clearDurableSession(MqttAsyncClient subscriber, Config config, int maxInflight) {
        try {
            if (subscriber.isConnected()) {
                disconnectAndWait(subscriber);
            }
            connectAndWait(subscriber, config, maxInflight, true, "clear durable session connect");
            disconnectAndWait(subscriber);
        } catch (MqttException e) {
            // best-effort cleanup
        }
    }

    private ScheduledFuture<?> scheduleReporter(ScheduledExecutorService reporter, Stats stats, Config config) {
        return reporter.scheduleAtFixedRate(stats::report,
                config.intervalSeconds, config.intervalSeconds, TimeUnit.SECONDS);
    }

    private void publish(Config config, MqttAsyncClient publisher, Semaphore inflight, Stats stats)
            throws MqttException, InterruptedException {
        // Pre-fill a padding template; clone + overwrite the 16-byte header per message.
        byte[] template = new byte[config.size];
        for (int i = Config.HEADER_SIZE; i < template.length; i++) {
            template[i] = (byte) ('a' + (i % 26));
        }

        for (long seq = 0; seq < config.count; seq++) {
            inflight.acquire();

            byte[] payload = template.clone();
            ByteBuffer header = ByteBuffer.wrap(payload, 0, Config.HEADER_SIZE);
            header.putLong(seq);
            header.putLong(System.nanoTime());

            MqttMessage message = new MqttMessage(payload);
            message.setQos(config.qos);
            message.setRetained(false);

            publisher.publish(config.topic, message);
            stats.recordSent(payload.length);

            long published = seq + 1;
            if (config.throttleEvery > 0 && published < config.count && published % config.throttleEvery == 0) {
                Thread.sleep(config.throttleDelayMs);
            }
        }
    }

    /** Wait until all expected messages arrive, or until the receive side goes idle (covers QoS 0 loss). */
    private void awaitReceive(Config config, Stats stats) throws InterruptedException {
        long idleTimeoutMs = receiveIdleTimeoutMs(config);
        long lastCount = -1;
        long lastProgress = System.currentTimeMillis();

        while (stats.receivedMessages() < config.count) {
            Thread.sleep(50);
            long current = stats.receivedMessages();
            if (current != lastCount) {
                lastCount = current;
                lastProgress = System.currentTimeMillis();
            } else if (System.currentTimeMillis() - lastProgress > idleTimeoutMs) {
                System.out.println("Receiver idle for " + idleTimeoutMs + " ms with "
                        + String.format("%,d", config.count - current)
                        + " messages still missing; assuming message loss.");
                break;
            }
        }
    }

    private long receiveIdleTimeoutMs(Config config) {
        if (config.receiveIdleTimeoutSeconds > 0) {
            return config.receiveIdleTimeoutSeconds * 1_000L;
        }
        if (config.qos == 0) {
            return Math.max(5_000L, config.intervalSeconds * 2_000L);
        }
        return Math.max(300_000L, config.intervalSeconds * 2_000L);
    }

    private void printTable(List<Result> results, Config config) {
        System.out.println();
        System.out.println("======================================== Throughput summary ========================================");
        System.out.printf("  Broker: %s   payload: %d B   messages: %,d per run%n",
                config.broker, config.size, config.count);
        System.out.println("---------------------------------------------------------------------------------------------------");
        System.out.printf("  %-11s %4s %15s %12s %15s %12s %9s %10s%n",
                "Mode", "QoS", "Send msg/s", "Send MB/s", "Recv msg/s", "Recv MB/s", "Lost", "In-seq");
        System.out.println("  -------------------------------------------------------------------------------------------------");
        for (Result r : results) {
            String sendMsg = r.sendMsgRate() < 0 ? "-" : String.format("%,.0f", r.sendMsgRate());
            String sendMb = r.sendMbRate() < 0 ? "-" : String.format("%.1f", r.sendMbRate());
            String inSeq = r.outOfOrder() == 0 ? "yes" : "no(" + r.outOfOrder() + ")";
            System.out.printf("  %-11s %4d %15s %12s %15s %12s %9d %10s%n",
                    r.mode(), r.qos(), sendMsg, sendMb,
                    String.format("%,.0f", r.recvMsgRate()),
                    String.format("%.1f", r.recvMbRate()),
                    r.lost(), inSeq);
        }
        System.out.println("===================================================================================================");
        System.out.println("  send-drain: Send = publish throughput (consumer offline); Recv = drain throughput.");
        System.out.println("  parallel  : Recv = end-to-end throughput (subscriber consuming during publish).");
        System.out.println("  In-seq    : yes = every message arrived in ascending sequence; no(N) = N out-of-order arrivals.");
    }

    private static MqttCallback receiveCallback(MqttAsyncClient subscriber, Stats stats) {
        return new MqttCallback() {
            @Override
            public void connectionLost(Throwable cause) {
                System.err.println("Subscriber connection lost: " + cause);
            }

            @Override
            public void messageArrived(String topic, MqttMessage message) throws MqttException {
                byte[] payload = message.getPayload();
                ByteBuffer header = ByteBuffer.wrap(payload, 0, Config.HEADER_SIZE);
                long seq = header.getLong();       // bytes 0..7
                long sentNanos = header.getLong(); // bytes 8..15
                subscriber.messageArrivedComplete(message.getId(), message.getQos());
                stats.recordReceived(payload.length, seq, System.nanoTime() - sentNanos);
            }

            @Override
            public void deliveryComplete(IMqttDeliveryToken token) { /* unused on subscriber */ }
        };
    }

    private static MqttCallback publishCallback(Semaphore inflight) {
        return new MqttCallback() {
            @Override
            public void connectionLost(Throwable cause) {
                System.err.println("Publisher connection lost: " + cause);
            }

            @Override
            public void messageArrived(String topic, MqttMessage message) { /* publisher does not subscribe */ }

            @Override
            public void deliveryComplete(IMqttDeliveryToken token) {
                inflight.release();
            }
        };
    }

    private static MqttConnectOptions connectOptions(Config config, int maxInflight, boolean cleanSession) {
        MqttConnectOptions options = new MqttConnectOptions();
        options.setMqttVersion(MqttConnectOptions.MQTT_VERSION_3_1_1);
        options.setCleanSession(cleanSession);
        options.setAutomaticReconnect(false);
        options.setMaxInflight(maxInflight);
        options.setConnectionTimeout(30);
        options.setKeepAliveInterval(60);
        if (config.username != null) {
            options.setUserName(config.username);
        }
        if (config.password != null) {
            options.setPassword(config.password.toCharArray());
        }
        return options;
    }

    private static void connectAndWait(MqttAsyncClient client, Config config, int maxInflight,
                                       boolean cleanSession, String operation) throws MqttException {
        IMqttToken token = client.connect(connectOptions(config, maxInflight, cleanSession));
        waitForCompletion(token, CONTROL_COMPLETION_TIMEOUT_MS, operation);
    }

    private static void subscribeAndWait(MqttAsyncClient client, String topic, int qos) throws MqttException {
        IMqttToken token = client.subscribe(topic, qos);
        waitForCompletion(token, CONTROL_COMPLETION_TIMEOUT_MS, "subscribe " + topic);

        int[] grantedQos = token.getGrantedQos();
        if (grantedQos == null || grantedQos.length == 0 || grantedQos[0] == 0x80) {
            throw new MqttException(MqttException.REASON_CODE_SUBSCRIBE_FAILED);
        }
    }

    private static void waitForCompletion(IMqttToken token, long timeoutMs, String operation) throws MqttException {
        token.waitForCompletion(timeoutMs);
        if (!token.isComplete()) {
            throw new MqttException(MqttException.REASON_CODE_CLIENT_TIMEOUT,
                    new TimeoutException(operation + " timed out after " + timeoutMs + " ms"));
        }
    }

    private static void disconnectAndClose(MqttAsyncClient client) {
        disconnectAndWait(client);
        try {
            client.close(true);
        } catch (MqttException e) {
            // best-effort cleanup
        }
    }

    private static void disconnectAndWait(MqttAsyncClient client) {
        try {
            if (client.isConnected()) {
                IMqttToken token = client.disconnect(DISCONNECT_QUIESCE_TIMEOUT_MS);
                waitForCompletion(token, DISCONNECT_COMPLETION_TIMEOUT_MS, "disconnect");
            }
        } catch (MqttException e) {
            try {
                client.disconnectForcibly(DISCONNECT_QUIESCE_TIMEOUT_MS,
                        DISCONNECT_COMPLETION_TIMEOUT_MS, true);
            } catch (MqttException ignored) {
                // best-effort cleanup
            }
        }
    }

    private static void forceOffline(MqttAsyncClient client) {
        try {
            if (client.isConnected()) {
                client.disconnectForcibly(0, DISCONNECT_COMPLETION_TIMEOUT_MS, false);
            }
        } catch (MqttException e) {
            // best-effort offline transition
        }
    }
}
