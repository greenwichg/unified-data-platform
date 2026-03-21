package com.zomato.pipeline1.patterns;

import com.zomato.pipeline1.events.OrderEvent;

import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Flink CEP pattern: detects potential fraud when the same user places
 * multiple orders from different delivery addresses within a 5-minute window.
 *
 * <p>Pattern logic:
 * <ol>
 *   <li>Match a first order for a given user (keyed by userId).</li>
 *   <li>Within 5 minutes, match a second order from the same user but with
 *       a different delivery address.</li>
 *   <li>Optionally, match a third distinct address within the same window
 *       to increase confidence.</li>
 * </ol>
 *
 * <p>Detected patterns are emitted as {@link FraudAlert} events.
 */
public class FraudDetectionPattern implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(FraudDetectionPattern.class);

    /** Maximum time window for the multi-address pattern. */
    private static final Time DETECTION_WINDOW = Time.minutes(5);

    /** Minimum number of distinct addresses to trigger an alert. */
    private static final int MIN_DISTINCT_ADDRESSES = 2;

    /** Minimum number of orders within the window to trigger an alert. */
    private static final int MIN_ORDERS = 3;

    // -----------------------------------------------------------------------
    // Alert POJO
    // -----------------------------------------------------------------------

    public static class FraudAlert implements Serializable {
        private static final long serialVersionUID = 1L;

        private String userId;
        private List<String> orderIds;
        private Set<String> distinctAddresses;
        private String alertType;
        private String severity;
        private long detectedAt;
        private long windowStartMs;
        private long windowEndMs;

        public FraudAlert() {
        }

        public String getUserId() { return userId; }
        public void setUserId(String userId) { this.userId = userId; }
        public List<String> getOrderIds() { return orderIds; }
        public void setOrderIds(List<String> orderIds) { this.orderIds = orderIds; }
        public Set<String> getDistinctAddresses() { return distinctAddresses; }
        public void setDistinctAddresses(Set<String> distinctAddresses) { this.distinctAddresses = distinctAddresses; }
        public String getAlertType() { return alertType; }
        public void setAlertType(String alertType) { this.alertType = alertType; }
        public String getSeverity() { return severity; }
        public void setSeverity(String severity) { this.severity = severity; }
        public long getDetectedAt() { return detectedAt; }
        public void setDetectedAt(long detectedAt) { this.detectedAt = detectedAt; }
        public long getWindowStartMs() { return windowStartMs; }
        public void setWindowStartMs(long windowStartMs) { this.windowStartMs = windowStartMs; }
        public long getWindowEndMs() { return windowEndMs; }
        public void setWindowEndMs(long windowEndMs) { this.windowEndMs = windowEndMs; }

        @Override
        public String toString() {
            return "FraudAlert{userId='" + userId + "', orders=" + orderIds.size()
                    + ", addresses=" + distinctAddresses.size()
                    + ", severity='" + severity + "'}";
        }
    }

    // -----------------------------------------------------------------------
    // Pattern definition
    // -----------------------------------------------------------------------

    /**
     * Builds the Flink CEP pattern that detects multiple orders from
     * different delivery addresses within a 5-minute window for the same user.
     */
    public static Pattern<OrderEvent, ?> buildPattern() {
        return Pattern.<OrderEvent>begin("firstOrder")
                .where(new IterativeCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent event, Context<OrderEvent> ctx) {
                        return event.getDeliveryAddress() != null
                                && !event.getDeliveryAddress().isEmpty()
                                && "PLACED".equalsIgnoreCase(event.getStatus());
                    }
                })
                .next("secondOrder")
                .where(new IterativeCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent event, Context<OrderEvent> ctx) throws Exception {
                        if (event.getDeliveryAddress() == null
                                || event.getDeliveryAddress().isEmpty()
                                || !"PLACED".equalsIgnoreCase(event.getStatus())) {
                            return false;
                        }
                        // Check that this order has a different address from the first
                        for (OrderEvent first : ctx.getEventsForPattern("firstOrder")) {
                            if (!normalizeAddress(first.getDeliveryAddress())
                                    .equals(normalizeAddress(event.getDeliveryAddress()))) {
                                return true;
                            }
                        }
                        return false;
                    }
                })
                .followedBy("subsequentOrders")
                .where(new IterativeCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent event, Context<OrderEvent> ctx) {
                        return event.getDeliveryAddress() != null
                                && !event.getDeliveryAddress().isEmpty()
                                && "PLACED".equalsIgnoreCase(event.getStatus());
                    }
                })
                .oneOrMore()
                .optional()
                .greedy()
                .within(DETECTION_WINDOW);
    }

    /**
     * Applies the fraud detection CEP pattern to an order event stream
     * keyed by userId and returns a stream of {@link FraudAlert} events.
     */
    public static DataStream<FraudAlert> apply(DataStream<OrderEvent> orderStream) {
        KeyedStream<OrderEvent, String> keyedByUser = orderStream
                .keyBy(OrderEvent::getUserId);

        Pattern<OrderEvent, ?> pattern = buildPattern();

        PatternStream<OrderEvent> patternStream = CEP.pattern(keyedByUser, pattern);

        return patternStream.select(new PatternSelectFunction<OrderEvent, FraudAlert>() {
            @Override
            public FraudAlert select(Map<String, List<OrderEvent>> match) {
                List<OrderEvent> firstOrders = match.get("firstOrder");
                List<OrderEvent> secondOrders = match.get("secondOrder");
                List<OrderEvent> subsequent = match.getOrDefault("subsequentOrders", new ArrayList<>());

                List<OrderEvent> allOrders = new ArrayList<>();
                allOrders.addAll(firstOrders);
                allOrders.addAll(secondOrders);
                allOrders.addAll(subsequent);

                Set<String> addresses = new HashSet<>();
                List<String> orderIds = new ArrayList<>();
                long minTimestamp = Long.MAX_VALUE;
                long maxTimestamp = Long.MIN_VALUE;

                for (OrderEvent evt : allOrders) {
                    orderIds.add(evt.getOrderId());
                    addresses.add(normalizeAddress(evt.getDeliveryAddress()));
                    minTimestamp = Math.min(minTimestamp, evt.getUpdatedAt());
                    maxTimestamp = Math.max(maxTimestamp, evt.getUpdatedAt());
                }

                String userId = allOrders.get(0).getUserId();
                String severity = determineSeverity(addresses.size(), allOrders.size());

                FraudAlert alert = new FraudAlert();
                alert.setUserId(userId);
                alert.setOrderIds(orderIds);
                alert.setDistinctAddresses(addresses);
                alert.setAlertType("MULTI_ADDRESS_FRAUD");
                alert.setSeverity(severity);
                alert.setDetectedAt(Instant.now().toEpochMilli());
                alert.setWindowStartMs(minTimestamp);
                alert.setWindowEndMs(maxTimestamp);

                LOG.warn("Fraud detected: userId={}, orders={}, addresses={}, severity={}",
                        userId, orderIds.size(), addresses.size(), severity);

                return alert;
            }
        });
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    /**
     * Normalize an address string for comparison: lowercase, trim, collapse whitespace.
     */
    private static String normalizeAddress(String address) {
        if (address == null) return "";
        return address.toLowerCase().trim().replaceAll("\\s+", " ");
    }

    /**
     * Determine alert severity based on the number of distinct addresses and order count.
     */
    private static String determineSeverity(int distinctAddresses, int orderCount) {
        if (distinctAddresses >= 4 || orderCount >= 6) {
            return "CRITICAL";
        } else if (distinctAddresses >= 3 || orderCount >= 4) {
            return "HIGH";
        } else if (distinctAddresses >= MIN_DISTINCT_ADDRESSES || orderCount >= MIN_ORDERS) {
            return "MEDIUM";
        }
        return "LOW";
    }
}
