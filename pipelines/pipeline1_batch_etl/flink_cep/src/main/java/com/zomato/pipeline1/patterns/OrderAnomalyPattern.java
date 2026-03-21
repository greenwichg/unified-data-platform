package com.zomato.pipeline1.patterns;

import com.zomato.pipeline1.events.OrderEvent;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.time.Instant;

/**
 * CEP pattern: detects a sudden spike in order cancellations for a restaurant.
 *
 * <p>Approach:
 * <ol>
 *   <li>Key the order stream by restaurantId.</li>
 *   <li>Filter for cancellation events (status = CANCELLED).</li>
 *   <li>Use a 10-minute sliding window (slide every 1 minute) to count cancellations.</li>
 *   <li>Compare the current window count against a rolling baseline stored in keyed state.</li>
 *   <li>If the count exceeds the baseline by a configurable multiplier (default 3x) or
 *       exceeds an absolute threshold, emit an anomaly alert.</li>
 * </ol>
 */
public class OrderAnomalyPattern implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(OrderAnomalyPattern.class);

    /** Window size for cancellation counting. */
    private static final Time WINDOW_SIZE = Time.minutes(10);
    /** Slide interval for the sliding window. */
    private static final Time SLIDE_INTERVAL = Time.minutes(1);
    /** Multiplier over baseline to trigger an alert. */
    private static final double SPIKE_MULTIPLIER = 3.0;
    /** Absolute minimum cancellations to trigger (avoids alerts on very low volume). */
    private static final long ABSOLUTE_THRESHOLD = 5;
    /** Exponential moving average decay factor for baseline. */
    private static final double EMA_ALPHA = 0.3;

    // -----------------------------------------------------------------------
    // Anomaly alert POJO
    // -----------------------------------------------------------------------

    public static class CancellationAnomalyAlert implements Serializable {
        private static final long serialVersionUID = 1L;

        private String restaurantId;
        private long cancellationCount;
        private double baselineCount;
        private double spikeRatio;
        private String severity;
        private long windowStartMs;
        private long windowEndMs;
        private long detectedAt;

        public CancellationAnomalyAlert() {
        }

        public String getRestaurantId() { return restaurantId; }
        public void setRestaurantId(String restaurantId) { this.restaurantId = restaurantId; }
        public long getCancellationCount() { return cancellationCount; }
        public void setCancellationCount(long cancellationCount) { this.cancellationCount = cancellationCount; }
        public double getBaselineCount() { return baselineCount; }
        public void setBaselineCount(double baselineCount) { this.baselineCount = baselineCount; }
        public double getSpikeRatio() { return spikeRatio; }
        public void setSpikeRatio(double spikeRatio) { this.spikeRatio = spikeRatio; }
        public String getSeverity() { return severity; }
        public void setSeverity(String severity) { this.severity = severity; }
        public long getWindowStartMs() { return windowStartMs; }
        public void setWindowStartMs(long windowStartMs) { this.windowStartMs = windowStartMs; }
        public long getWindowEndMs() { return windowEndMs; }
        public void setWindowEndMs(long windowEndMs) { this.windowEndMs = windowEndMs; }
        public long getDetectedAt() { return detectedAt; }
        public void setDetectedAt(long detectedAt) { this.detectedAt = detectedAt; }

        @Override
        public String toString() {
            return "CancellationAnomalyAlert{restaurantId='" + restaurantId
                    + "', count=" + cancellationCount
                    + ", baseline=" + String.format("%.1f", baselineCount)
                    + ", ratio=" + String.format("%.2f", spikeRatio)
                    + ", severity='" + severity + "'}";
        }
    }

    // -----------------------------------------------------------------------
    // Internal: window count result
    // -----------------------------------------------------------------------

    private static class WindowCancellationCount implements Serializable {
        private static final long serialVersionUID = 1L;
        String restaurantId;
        long count;
        long windowStart;
        long windowEnd;

        WindowCancellationCount(String restaurantId, long count, long windowStart, long windowEnd) {
            this.restaurantId = restaurantId;
            this.count = count;
            this.windowStart = windowStart;
            this.windowEnd = windowEnd;
        }
    }

    // -----------------------------------------------------------------------
    // Pattern application
    // -----------------------------------------------------------------------

    /**
     * Applies the cancellation anomaly detection pattern to the order stream.
     *
     * @param orderStream stream of all order events
     * @return stream of anomaly alerts
     */
    public static DataStream<CancellationAnomalyAlert> apply(DataStream<OrderEvent> orderStream) {
        // Step 1: Filter cancellations and key by restaurant
        DataStream<WindowCancellationCount> windowedCounts = orderStream
                .filter(e -> "CANCELLED".equalsIgnoreCase(e.getStatus()))
                .keyBy(OrderEvent::getRestaurantId)
                .window(SlidingEventTimeWindows.of(WINDOW_SIZE, SLIDE_INTERVAL))
                .aggregate(
                        new CancellationCounter(),
                        new CancellationWindowFunction()
                );

        // Step 2: Compare against EMA baseline and emit alerts
        return windowedCounts
                .keyBy(wc -> wc.restaurantId)
                .process(new BaselineComparisonFunction());
    }

    // -----------------------------------------------------------------------
    // Aggregate function: count cancellations per window
    // -----------------------------------------------------------------------

    private static class CancellationCounter
            implements AggregateFunction<OrderEvent, Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(OrderEvent event, Long acc) {
            return acc + 1;
        }

        @Override
        public Long getResult(Long acc) {
            return acc;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a + b;
        }
    }

    // -----------------------------------------------------------------------
    // Process window function: attach window metadata
    // -----------------------------------------------------------------------

    private static class CancellationWindowFunction
            extends ProcessWindowFunction<Long, WindowCancellationCount, String, TimeWindow> {

        @Override
        public void process(String restaurantId,
                            ProcessWindowFunction<Long, WindowCancellationCount, String, TimeWindow>.Context ctx,
                            Iterable<Long> counts,
                            Collector<WindowCancellationCount> out) {
            long count = counts.iterator().next();
            TimeWindow window = ctx.window();
            out.collect(new WindowCancellationCount(
                    restaurantId, count, window.getStart(), window.getEnd()));
        }
    }

    // -----------------------------------------------------------------------
    // Keyed process function: EMA baseline comparison
    // -----------------------------------------------------------------------

    private static class BaselineComparisonFunction
            extends KeyedProcessFunction<String, WindowCancellationCount, CancellationAnomalyAlert> {

        private transient ValueState<Double> baselineState;

        @Override
        public void open(Configuration parameters) {
            ValueStateDescriptor<Double> descriptor = new ValueStateDescriptor<>(
                    "cancellation-baseline", Types.DOUBLE);
            baselineState = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void processElement(WindowCancellationCount value,
                                   KeyedProcessFunction<String, WindowCancellationCount, CancellationAnomalyAlert>.Context ctx,
                                   Collector<CancellationAnomalyAlert> out) throws Exception {

            Double currentBaseline = baselineState.value();

            if (currentBaseline == null) {
                // First window for this restaurant; initialize baseline
                baselineState.update((double) value.count);
                return;
            }

            // Check for spike
            boolean absoluteSpike = value.count >= ABSOLUTE_THRESHOLD;
            double spikeRatio = currentBaseline > 0
                    ? (double) value.count / currentBaseline
                    : value.count;
            boolean relativeSpike = spikeRatio >= SPIKE_MULTIPLIER;

            if (absoluteSpike && relativeSpike) {
                CancellationAnomalyAlert alert = new CancellationAnomalyAlert();
                alert.setRestaurantId(value.restaurantId);
                alert.setCancellationCount(value.count);
                alert.setBaselineCount(currentBaseline);
                alert.setSpikeRatio(spikeRatio);
                alert.setSeverity(determineSeverity(spikeRatio));
                alert.setWindowStartMs(value.windowStart);
                alert.setWindowEndMs(value.windowEnd);
                alert.setDetectedAt(Instant.now().toEpochMilli());

                LOG.warn("Cancellation spike detected: {}", alert);
                out.collect(alert);
            }

            // Update EMA baseline
            double newBaseline = EMA_ALPHA * value.count + (1 - EMA_ALPHA) * currentBaseline;
            baselineState.update(newBaseline);
        }

        private static String determineSeverity(double spikeRatio) {
            if (spikeRatio >= 10.0) {
                return "CRITICAL";
            } else if (spikeRatio >= 5.0) {
                return "HIGH";
            } else if (spikeRatio >= SPIKE_MULTIPLIER) {
                return "MEDIUM";
            }
            return "LOW";
        }
    }
}
