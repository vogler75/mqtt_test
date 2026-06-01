package de.iitsoftware.mqtttest;

/**
 * One row of the benchmark matrix: the throughput a single (mode, QoS) run achieved.
 *
 * <p>For parallel runs the send rates are {@code -1} (not applicable — the headline number is the
 * end-to-end receive rate). For send-drain runs both the send-phase and drain-phase rates are set.
 * {@code outOfOrder} is the number of messages that arrived with a lower sequence number than their
 * predecessor (0 means strictly in-order delivery).</p>
 */
public record Result(
        String mode,
        int qos,
        double sendMsgRate,
        double sendMbRate,
        double recvMsgRate,
        double recvMbRate,
        long lost,
        long outOfOrder) {
}
