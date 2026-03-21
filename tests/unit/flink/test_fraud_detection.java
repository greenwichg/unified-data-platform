package com.zomato.dataplatform.flink;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.test.util.MiniClusterResourceConfiguration;
import org.junit.ClassRule;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.*;

import static org.junit.Assert.*;

/**
 * JUnit tests for the Flink CEP fraud detection pattern used in Pipeline 4.
 *
 * The pattern detects:
 *  1. Rapid-fire orders: > 3 orders from the same user within 2 minutes
 *  2. High-value spike: single order > 10x the user's average order value
 *  3. Geo-impossible: orders from cities > 500 km apart within 5 minutes
 *  4. Promo abuse: same promo code used by same device fingerprint > 3 times
 */
public class test_fraud_detection {

    @ClassRule
    public static MiniClusterWithClientResource flinkCluster =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberSlotsPerTaskManager(2)
                            .setNumberTaskManagers(1)
                            .build());

    // Shared sink to collect results across tests
    private static final List<FraudAlert> collectedAlerts =
            Collections.synchronizedList(new ArrayList<>());

    @Before
    public void setUp() {
        collectedAlerts.clear();
    }

    // -----------------------------------------------------------------------
    // Domain objects
    // -----------------------------------------------------------------------
    public static class OrderEvent implements java.io.Serializable {
        public String eventId;
        public String userId;
        public String orderId;
        public String city;
        public BigDecimal totalAmount;
        public String paymentMethod;
        public String promoCode;
        public String deviceFingerprint;
        public double latitude;
        public double longitude;
        public long timestampMs;

        public OrderEvent() {}

        public OrderEvent(String userId, String city, BigDecimal totalAmount,
                          long timestampMs) {
            this.eventId = UUID.randomUUID().toString();
            this.userId = userId;
            this.orderId = UUID.randomUUID().toString();
            this.city = city;
            this.totalAmount = totalAmount;
            this.paymentMethod = "UPI";
            this.promoCode = null;
            this.deviceFingerprint = "device-" + userId;
            this.latitude = 19.076;
            this.longitude = 72.877;
            this.timestampMs = timestampMs;
        }
    }

    public static class FraudAlert implements java.io.Serializable {
        public String alertType;
        public String userId;
        public String details;
        public List<String> orderIds;
        public long detectedAt;

        public FraudAlert() {}

        public FraudAlert(String alertType, String userId, String details,
                          List<String> orderIds) {
            this.alertType = alertType;
            this.userId = userId;
            this.details = details;
            this.orderIds = orderIds;
            this.detectedAt = Instant.now().toEpochMilli();
        }
    }

    // -----------------------------------------------------------------------
    // Sink for test collection
    // -----------------------------------------------------------------------
    public static class CollectSink implements SinkFunction<FraudAlert> {
        @Override
        public void invoke(FraudAlert value, Context context) {
            collectedAlerts.add(value);
        }
    }

    // -----------------------------------------------------------------------
    // Pattern definitions (mirrors production logic)
    // -----------------------------------------------------------------------

    /**
     * Rapid-fire orders pattern: 4+ orders from the same user within 2 minutes.
     */
    private Pattern<OrderEvent, ?> rapidFirePattern() {
        return Pattern.<OrderEvent>begin("first")
                .where(new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent event) {
                        return event.userId != null;
                    }
                })
                .next("second")
                .where(new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent event) {
                        return event.userId != null;
                    }
                })
                .next("third")
                .where(new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent event) {
                        return event.userId != null;
                    }
                })
                .next("fourth")
                .where(new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent event) {
                        return event.userId != null;
                    }
                })
                .within(Time.minutes(2));
    }

    /**
     * High-value spike pattern: single order exceeding a threshold amount.
     */
    private Pattern<OrderEvent, ?> highValuePattern(BigDecimal threshold) {
        return Pattern.<OrderEvent>begin("highValue")
                .where(new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent event) {
                        return event.totalAmount.compareTo(threshold) > 0;
                    }
                });
    }

    /**
     * Promo abuse pattern: same promo code used 4+ times within 10 minutes.
     */
    private Pattern<OrderEvent, ?> promoAbusePattern() {
        return Pattern.<OrderEvent>begin("promo1")
                .where(new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent event) {
                        return event.promoCode != null && !event.promoCode.isEmpty();
                    }
                })
                .next("promo2")
                .where(new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent event) {
                        return event.promoCode != null && !event.promoCode.isEmpty();
                    }
                })
                .next("promo3")
                .where(new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent event) {
                        return event.promoCode != null && !event.promoCode.isEmpty();
                    }
                })
                .next("promo4")
                .where(new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent event) {
                        return event.promoCode != null && !event.promoCode.isEmpty();
                    }
                })
                .within(Time.minutes(10));
    }

    // -----------------------------------------------------------------------
    // Tests
    // -----------------------------------------------------------------------

    @Test
    public void testRapidFireDetection_shouldAlert() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        long now = Instant.now().toEpochMilli();
        // 4 orders within 30 seconds from the same user
        List<OrderEvent> events = Arrays.asList(
                new OrderEvent("usr-fraud-01", "Mumbai", new BigDecimal("300"), now),
                new OrderEvent("usr-fraud-01", "Mumbai", new BigDecimal("450"), now + 10_000),
                new OrderEvent("usr-fraud-01", "Mumbai", new BigDecimal("200"), now + 20_000),
                new OrderEvent("usr-fraud-01", "Mumbai", new BigDecimal("550"), now + 30_000)
        );

        DataStream<OrderEvent> input = env.fromCollection(events);

        PatternStream<OrderEvent> patternStream = CEP.pattern(
                input.keyBy(e -> e.userId), rapidFirePattern());

        DataStream<FraudAlert> alerts = patternStream.select(
                (PatternSelectFunction<OrderEvent, FraudAlert>) pattern -> {
                    List<String> orderIds = new ArrayList<>();
                    for (Map.Entry<String, List<OrderEvent>> entry : pattern.entrySet()) {
                        for (OrderEvent e : entry.getValue()) {
                            orderIds.add(e.orderId);
                        }
                    }
                    OrderEvent first = pattern.get("first").get(0);
                    return new FraudAlert(
                            "RAPID_FIRE_ORDERS",
                            first.userId,
                            "4 orders within 2 minutes",
                            orderIds
                    );
                });

        alerts.addSink(new CollectSink());
        env.execute("Rapid Fire Detection Test");

        assertFalse("Should detect rapid-fire orders", collectedAlerts.isEmpty());
        assertEquals("RAPID_FIRE_ORDERS", collectedAlerts.get(0).alertType);
        assertEquals("usr-fraud-01", collectedAlerts.get(0).userId);
        assertEquals(4, collectedAlerts.get(0).orderIds.size());
    }

    @Test
    public void testRapidFireDetection_normalPace_noAlert() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        long now = Instant.now().toEpochMilli();
        // Only 2 orders (below threshold of 4)
        List<OrderEvent> events = Arrays.asList(
                new OrderEvent("usr-normal-01", "Delhi", new BigDecimal("300"), now),
                new OrderEvent("usr-normal-01", "Delhi", new BigDecimal("450"), now + 60_000)
        );

        DataStream<OrderEvent> input = env.fromCollection(events);
        PatternStream<OrderEvent> patternStream = CEP.pattern(
                input.keyBy(e -> e.userId), rapidFirePattern());

        DataStream<FraudAlert> alerts = patternStream.select(
                (PatternSelectFunction<OrderEvent, FraudAlert>) pattern -> {
                    OrderEvent first = pattern.get("first").get(0);
                    return new FraudAlert("RAPID_FIRE_ORDERS", first.userId, "", List.of());
                });

        alerts.addSink(new CollectSink());
        env.execute("Normal Pace Test");

        assertTrue("Should not fire for 2 orders", collectedAlerts.isEmpty());
    }

    @Test
    public void testHighValueSpike_shouldAlert() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        BigDecimal threshold = new BigDecimal("50000");
        long now = Instant.now().toEpochMilli();

        List<OrderEvent> events = Arrays.asList(
                new OrderEvent("usr-hv-01", "Mumbai", new BigDecimal("500"), now),
                new OrderEvent("usr-hv-01", "Mumbai", new BigDecimal("75000"), now + 5_000),
                new OrderEvent("usr-hv-01", "Mumbai", new BigDecimal("600"), now + 10_000)
        );

        DataStream<OrderEvent> input = env.fromCollection(events);
        PatternStream<OrderEvent> patternStream = CEP.pattern(
                input.keyBy(e -> e.userId), highValuePattern(threshold));

        DataStream<FraudAlert> alerts = patternStream.select(
                (PatternSelectFunction<OrderEvent, FraudAlert>) pattern -> {
                    OrderEvent hv = pattern.get("highValue").get(0);
                    return new FraudAlert(
                            "HIGH_VALUE_SPIKE",
                            hv.userId,
                            "Order amount " + hv.totalAmount + " exceeds threshold " + threshold,
                            List.of(hv.orderId)
                    );
                });

        alerts.addSink(new CollectSink());
        env.execute("High Value Spike Test");

        assertEquals(1, collectedAlerts.size());
        assertEquals("HIGH_VALUE_SPIKE", collectedAlerts.get(0).alertType);
        assertEquals("usr-hv-01", collectedAlerts.get(0).userId);
        assertTrue(collectedAlerts.get(0).details.contains("75000"));
    }

    @Test
    public void testHighValueSpike_belowThreshold_noAlert() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        BigDecimal threshold = new BigDecimal("50000");
        long now = Instant.now().toEpochMilli();

        List<OrderEvent> events = Arrays.asList(
                new OrderEvent("usr-hv-02", "Delhi", new BigDecimal("300"), now),
                new OrderEvent("usr-hv-02", "Delhi", new BigDecimal("1500"), now + 5_000)
        );

        DataStream<OrderEvent> input = env.fromCollection(events);
        PatternStream<OrderEvent> patternStream = CEP.pattern(
                input.keyBy(e -> e.userId), highValuePattern(threshold));

        DataStream<FraudAlert> alerts = patternStream.select(
                (PatternSelectFunction<OrderEvent, FraudAlert>) pattern -> {
                    OrderEvent hv = pattern.get("highValue").get(0);
                    return new FraudAlert("HIGH_VALUE_SPIKE", hv.userId, "", List.of(hv.orderId));
                });

        alerts.addSink(new CollectSink());
        env.execute("Below Threshold Test");

        assertTrue("No alert for orders below threshold", collectedAlerts.isEmpty());
    }

    @Test
    public void testPromoAbuse_shouldAlert() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        long now = Instant.now().toEpochMilli();
        OrderEvent e1 = new OrderEvent("usr-promo-01", "Bangalore", new BigDecimal("200"), now);
        e1.promoCode = "FLAT50";
        OrderEvent e2 = new OrderEvent("usr-promo-01", "Bangalore", new BigDecimal("200"), now + 60_000);
        e2.promoCode = "FLAT50";
        OrderEvent e3 = new OrderEvent("usr-promo-01", "Bangalore", new BigDecimal("200"), now + 120_000);
        e3.promoCode = "FLAT50";
        OrderEvent e4 = new OrderEvent("usr-promo-01", "Bangalore", new BigDecimal("200"), now + 180_000);
        e4.promoCode = "FLAT50";

        DataStream<OrderEvent> input = env.fromCollection(Arrays.asList(e1, e2, e3, e4));
        PatternStream<OrderEvent> patternStream = CEP.pattern(
                input.keyBy(e -> e.userId), promoAbusePattern());

        DataStream<FraudAlert> alerts = patternStream.select(
                (PatternSelectFunction<OrderEvent, FraudAlert>) pattern -> {
                    OrderEvent first = pattern.get("promo1").get(0);
                    return new FraudAlert(
                            "PROMO_ABUSE",
                            first.userId,
                            "Promo " + first.promoCode + " used 4+ times in 10 min",
                            List.of()
                    );
                });

        alerts.addSink(new CollectSink());
        env.execute("Promo Abuse Test");

        assertFalse("Should detect promo abuse", collectedAlerts.isEmpty());
        assertEquals("PROMO_ABUSE", collectedAlerts.get(0).alertType);
    }

    @Test
    public void testMultipleUsers_isolatedDetection() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        long now = Instant.now().toEpochMilli();
        // User A: 4 rapid orders (should trigger)
        // User B: 2 orders (should NOT trigger)
        List<OrderEvent> events = Arrays.asList(
                new OrderEvent("usr-A", "Mumbai", new BigDecimal("300"), now),
                new OrderEvent("usr-B", "Delhi", new BigDecimal("500"), now + 1_000),
                new OrderEvent("usr-A", "Mumbai", new BigDecimal("300"), now + 5_000),
                new OrderEvent("usr-A", "Mumbai", new BigDecimal("300"), now + 10_000),
                new OrderEvent("usr-B", "Delhi", new BigDecimal("500"), now + 12_000),
                new OrderEvent("usr-A", "Mumbai", new BigDecimal("300"), now + 15_000)
        );

        DataStream<OrderEvent> input = env.fromCollection(events);
        PatternStream<OrderEvent> patternStream = CEP.pattern(
                input.keyBy(e -> e.userId), rapidFirePattern());

        DataStream<FraudAlert> alerts = patternStream.select(
                (PatternSelectFunction<OrderEvent, FraudAlert>) pattern -> {
                    OrderEvent first = pattern.get("first").get(0);
                    return new FraudAlert("RAPID_FIRE_ORDERS", first.userId, "", List.of());
                });

        alerts.addSink(new CollectSink());
        env.execute("Multi-User Isolation Test");

        // Only user A should trigger
        List<String> alertedUsers = new ArrayList<>();
        for (FraudAlert alert : collectedAlerts) {
            alertedUsers.add(alert.userId);
        }
        assertTrue("User A should trigger alert", alertedUsers.contains("usr-A"));
        assertFalse("User B should NOT trigger alert", alertedUsers.contains("usr-B"));
    }

    @Test
    public void testGeoDistanceCalculation() {
        // Utility test: verify haversine distance between Mumbai and Delhi
        double lat1 = 19.0760, lon1 = 72.8777;  // Mumbai
        double lat2 = 28.7041, lon2 = 77.1025;  // Delhi

        double distance = haversineKm(lat1, lon1, lat2, lon2);

        // Mumbai-Delhi is approximately 1,150-1,200 km
        assertTrue("Distance should be > 1000 km", distance > 1000);
        assertTrue("Distance should be < 1300 km", distance < 1300);
    }

    // -----------------------------------------------------------------------
    // Utility
    // -----------------------------------------------------------------------
    private double haversineKm(double lat1, double lon1, double lat2, double lon2) {
        double R = 6371.0; // Earth's radius in km
        double dLat = Math.toRadians(lat2 - lat1);
        double dLon = Math.toRadians(lon2 - lon1);
        double a = Math.sin(dLat / 2) * Math.sin(dLat / 2)
                + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
                * Math.sin(dLon / 2) * Math.sin(dLon / 2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        return R * c;
    }
}
