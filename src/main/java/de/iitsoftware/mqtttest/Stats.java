package de.iitsoftware.mqtttest;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Thread-safe counters for the throughput test plus a per-phase reporter.
 *
 * <p>The publisher thread updates the {@code sent*} counters; Paho's callback thread(s) update the
 * {@code received*} and latency counters. The reporter thread reads them.</p>
 *
 * <p>Reporting is organised into <em>phases</em>. {@link #beginPhase(boolean)} starts a phase that
 * tracks either the sent or the received counters; {@link #report()} then prints window and
 * phase-cumulative rates for whichever side the active phase tracks. This lets the send-drain mode
 * report a send phase (sent counters) and a drain phase (received counters) with the same machinery
 * the parallel mode uses for its single received phase.</p>
 */
public final class Stats {

    private final AtomicLong sentMessages = new AtomicLong();
    private final AtomicLong sentBytes = new AtomicLong();
    private final AtomicLong receivedMessages = new AtomicLong();
    private final AtomicLong receivedBytes = new AtomicLong();

    // End-to-end latency (nanoseconds), accumulated on the receive side.
    private final AtomicLong latencySumNanos = new AtomicLong();
    private final AtomicLong latencyMinNanos = new AtomicLong(Long.MAX_VALUE);
    private final AtomicLong latencyMaxNanos = new AtomicLong(Long.MIN_VALUE);

    // In-order arrival check. Messages are published with strictly increasing sequence numbers, so a
    // received sequence number lower than its predecessor's means the broker delivered out of order.
    // Gaps (from QoS 0 loss) are not reordering and do not count. lastSeq is written only from the
    // single Paho callback thread; outOfOrder is read by the reporter/summary threads.
    private final AtomicLong outOfOrder = new AtomicLong();
    private volatile long lastSeq = -1;

    // Active reporting phase.
    private volatile boolean trackSent;
    private long phaseStartNanos;
    private long windowStartNanos;
    private long windowStartCount;
    private long windowStartBytes;

    public void recordSent(int bytes) {
        sentMessages.incrementAndGet();
        sentBytes.addAndGet(bytes);
    }

    public void recordReceived(int bytes, long seq, long latencyNanos) {
        if (seq < lastSeq) {
            outOfOrder.incrementAndGet();
        }
        lastSeq = seq;
        receivedMessages.incrementAndGet();
        receivedBytes.addAndGet(bytes);
        latencySumNanos.addAndGet(latencyNanos);
        updateMin(latencyMinNanos, latencyNanos);
        updateMax(latencyMaxNanos, latencyNanos);
    }

    /** Number of messages that arrived with a lower sequence number than their predecessor. */
    public long outOfOrder() {
        return outOfOrder.get();
    }

    public long receivedMessages() {
        return receivedMessages.get();
    }

    public long sentMessages() {
        return sentMessages.get();
    }

    /** Start a reporting phase. {@code trackSent} selects which counters {@link #report()} prints. */
    public synchronized void beginPhase(boolean trackSent) {
        this.trackSent = trackSent;
        long now = System.nanoTime();
        phaseStartNanos = now;
        windowStartNanos = now;
        windowStartCount = phaseCount();
        windowStartBytes = phaseBytes();
    }

    /** Print an incremental throughput line for the active phase's counter. */
    public synchronized void report() {
        long now = System.nanoTime();
        long count = phaseCount();
        long bytes = phaseBytes();

        double windowSecs = (now - windowStartNanos) / 1e9;
        double phaseSecs = (now - phaseStartNanos) / 1e9;

        long windowCount = count - windowStartCount;
        long windowBytes = bytes - windowStartBytes;

        double winMsgRate = windowSecs > 0 ? windowCount / windowSecs : 0;
        double winMbRate = windowSecs > 0 ? (windowBytes / 1e6) / windowSecs : 0;
        double totMsgRate = phaseSecs > 0 ? count / phaseSecs : 0;
        double totMbRate = phaseSecs > 0 ? (bytes / 1e6) / phaseSecs : 0;

        String verb = trackSent ? "sent" : "recv";
        System.out.printf(
                "[%6.1fs] %s %,d msgs | window: %,10.0f msg/s  %7.2f MB/s | total: %,10.0f msg/s  %7.2f MB/s%n",
                phaseSecs, verb, count, winMsgRate, winMbRate, totMsgRate, totMbRate);

        windowStartNanos = now;
        windowStartCount = count;
        windowStartBytes = bytes;
    }

    /** Final summary for the parallel mode: send and receive side together, with latency. */
    public void printParallelSummary(long expected) {
        double secs = phaseSeconds();
        long sMsgs = sentMessages.get();
        long rMsgs = receivedMessages.get();
        long rBytes = receivedBytes.get();
        long lost = Math.max(0, expected - rMsgs);

        System.out.println();
        System.out.println("================ Summary (parallel) ================");
        System.out.printf("  Duration       : %.2f s%n", secs);
        System.out.printf("  Messages sent  : %,d%n", sMsgs);
        printReceiveStats(secs, rMsgs, rBytes, lost, expected, true);
        System.out.println("====================================================");
    }

    /** Summary for the send-drain mode's send phase: pure publish throughput. */
    public void printSendSummary() {
        double secs = phaseSeconds();
        long msgs = sentMessages.get();
        long bytes = sentBytes.get();
        double msgRate = secs > 0 ? msgs / secs : 0;
        double mbRate = secs > 0 ? (bytes / 1e6) / secs : 0;

        System.out.println();
        System.out.println("---------------- Send phase ----------------");
        System.out.printf("  Duration       : %.2f s%n", secs);
        System.out.printf("  Messages sent  : %,d%n", msgs);
        System.out.printf("  Bytes sent     : %,d (%.2f MB)%n", bytes, bytes / 1e6);
        System.out.printf("  Send rate      : %,.0f msg/s  |  %.2f MB/s%n", msgRate, mbRate);
        System.out.println("--------------------------------------------");
    }

    /** Summary for the send-drain mode's drain phase: pure receive throughput (no latency: queue-dominated). */
    public void printDrainSummary(long expected) {
        double secs = phaseSeconds();
        long rMsgs = receivedMessages.get();
        long rBytes = receivedBytes.get();
        long lost = Math.max(0, expected - rMsgs);

        System.out.println();
        System.out.println("---------------- Drain phase ---------------");
        System.out.printf("  Duration       : %.2f s%n", secs);
        printReceiveStats(secs, rMsgs, rBytes, lost, expected, false);
        System.out.println("--------------------------------------------");
    }

    private void printReceiveStats(double secs, long rMsgs, long rBytes, long lost, long expected, boolean latency) {
        double msgRate = secs > 0 ? rMsgs / secs : 0;
        double mbRate = secs > 0 ? (rBytes / 1e6) / secs : 0;
        System.out.printf("  Messages recv  : %,d%n", rMsgs);
        System.out.printf("  Messages lost  : %,d (%.3f%%)%n",
                lost, expected > 0 ? 100.0 * lost / expected : 0.0);
        System.out.printf("  Bytes recv     : %,d (%.2f MB)%n", rBytes, rBytes / 1e6);
        System.out.printf("  Recv rate      : %,.0f msg/s  |  %.2f MB/s%n", msgRate, mbRate);
        long ooo = outOfOrder.get();
        System.out.printf("  In sequence    : %s%n",
                ooo == 0 ? "yes" : "NO (" + String.format("%,d", ooo) + " out of order)");
        if (latency && rMsgs > 0) {
            System.out.printf("  Latency (e2e)  : min %.3f ms  avg %.3f ms  max %.3f ms%n",
                    latencyMinNanos.get() / 1e6,
                    (latencySumNanos.get() / (double) rMsgs) / 1e6,
                    latencyMaxNanos.get() / 1e6);
        }
    }

    /** Message rate (msg/s) for the active phase so far. Call after a phase completes to capture it. */
    public synchronized double phaseMsgRate() {
        double secs = phaseSeconds();
        return secs > 0 ? phaseCount() / secs : 0;
    }

    /** Data rate (MB/s) for the active phase so far. */
    public synchronized double phaseMbRate() {
        double secs = phaseSeconds();
        return secs > 0 ? (phaseBytes() / 1e6) / secs : 0;
    }

    private long phaseCount() {
        return trackSent ? sentMessages.get() : receivedMessages.get();
    }

    private long phaseBytes() {
        return trackSent ? sentBytes.get() : receivedBytes.get();
    }

    private synchronized double phaseSeconds() {
        return (System.nanoTime() - phaseStartNanos) / 1e9;
    }

    private static void updateMin(AtomicLong target, long value) {
        long prev;
        while (value < (prev = target.get())) {
            if (target.compareAndSet(prev, value)) return;
        }
    }

    private static void updateMax(AtomicLong target, long value) {
        long prev;
        while (value > (prev = target.get())) {
            if (target.compareAndSet(prev, value)) return;
        }
    }
}
