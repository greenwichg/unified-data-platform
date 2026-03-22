package com.zomato.pipeline1;

import com.zomato.pipeline1.events.OrderEvent;
import com.zomato.pipeline1.patterns.FraudDetectionPattern;
import com.zomato.pipeline1.patterns.FraudDetectionPattern.FraudAlert;
import com.zomato.pipeline1.patterns.OrderAnomalyPattern;
import com.zomato.pipeline1.patterns.OrderAnomalyPattern.CancellationAnomalyAlert;
import com.zomato.pipeline1.sinks.IcebergSink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Properties;

/**
 * Pipeline 1 - Batch CEP Job
 *
 * <p>Reads order events from Kafka, applies Flink CEP patterns for fraud detection
 * and order anomaly detection, then writes pattern matches to Apache Iceberg
 * tables on S3 in ORC format.</p>
 *
 * <p>CEP Patterns:
 * <ul>
 *   <li><b>Fraud Detection:</b> Multiple orders from different delivery addresses
 *       within 5 minutes for the same user.</li>
 *   <li><b>Cancellation Anomaly:</b> Sudden spike in cancellations per restaurant
 *       compared to a rolling baseline.</li>
 * </ul>
 *
 * <p>Configuration is read from flink-conf.yaml in the classpath or passed
 * via Flink's standard configuration mechanisms.</p>
 */
public class BatchCEPJob {

    private static final Logger LOG = LoggerFactory.getLogger(BatchCEPJob.class);

    // Default config values; overridden by flink-conf.yaml or system properties
    private static final String DEFAULT_KAFKA_BOOTSTRAP = "b-1.msk-zomato-data.kafka.ap-south-1.amazonaws.com:9098,b-2.msk-zomato-data.kafka.ap-south-1.amazonaws.com:9098,b-3.msk-zomato-data.kafka.ap-south-1.amazonaws.com:9098";
    private static final String DEFAULT_KAFKA_TOPIC = "orders";
    private static final String DEFAULT_CONSUMER_GROUP = "pipeline1-batch-cep";
    private static final String DEFAULT_S3_BUCKET = "zomato-data-platform-prod-raw-data-lake";
    private static final String DEFAULT_CHECKPOINT_DIR =
            "s3://zomato-data-platform-prod-checkpoints/flink/pipeline1";

    public static void main(String[] args) throws Exception {
        LOG.info("Starting Pipeline 1 - Batch CEP Job");

        // ---------------------------------------------------------------
        // Configuration
        // ---------------------------------------------------------------
        String kafkaBootstrap = getConfig("kafka.bootstrap.servers", DEFAULT_KAFKA_BOOTSTRAP);
        String kafkaTopic = getConfig("kafka.topic", DEFAULT_KAFKA_TOPIC);
        String consumerGroup = getConfig("kafka.consumer.group", DEFAULT_CONSUMER_GROUP);
        String s3Bucket = getConfig("s3.bucket", DEFAULT_S3_BUCKET);
        String checkpointDir = getConfig("checkpoint.dir", DEFAULT_CHECKPOINT_DIR);
        int parallelism = Integer.parseInt(getConfig("job.parallelism", "32"));

        // ---------------------------------------------------------------
        // Execution environment
        // ---------------------------------------------------------------
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);

        // Checkpointing: exactly-once with RocksDB state backend
        env.enableCheckpointing(60_000L, CheckpointingMode.EXACTLY_ONCE);
        CheckpointConfig cpConfig = env.getCheckpointConfig();
        cpConfig.setMinPauseBetweenCheckpoints(30_000L);
        cpConfig.setCheckpointTimeout(600_000L);
        cpConfig.setMaxConcurrentCheckpoints(1);
        cpConfig.setTolerableCheckpointFailureNumber(3);
        cpConfig.setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.getCheckpointConfig().setCheckpointStorage(checkpointDir);

        // Restart strategy: fixed-delay
        env.setRestartStrategy(
                org.apache.flink.api.common.restartstrategy.RestartStrategies
                        .fixedDelayRestart(10, org.apache.flink.api.common.time.Time.seconds(30)));

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // ---------------------------------------------------------------
        // Kafka source
        // ---------------------------------------------------------------
        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("security.protocol", "SASL_SSL");
        kafkaProps.setProperty("sasl.mechanism", "AWS_MSK_IAM");
        kafkaProps.setProperty("sasl.jaas.config", "software.amazon.msk.auth.iam.IAMLoginModule required;");
        kafkaProps.setProperty("sasl.client.callback.handler.class", "software.amazon.msk.auth.iam.IAMClientCallbackHandler");
        kafkaProps.setProperty("fetch.min.bytes", "1048576");
        kafkaProps.setProperty("fetch.max.wait.ms", "500");
        kafkaProps.setProperty("max.poll.records", "10000");

        KafkaSource<OrderEvent> kafkaSource = KafkaSource.<OrderEvent>builder()
                .setBootstrapServers(kafkaBootstrap)
                .setTopics(kafkaTopic)
                .setGroupId(consumerGroup)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new OrderEventDeserializer())
                .setProperties(kafkaProps)
                .build();

        WatermarkStrategy<OrderEvent> watermarkStrategy = WatermarkStrategy
                .<OrderEvent>forBoundedOutOfOrderness(Duration.ofSeconds(30))
                .withTimestampAssigner((event, timestamp) -> event.getUpdatedAt())
                .withIdleness(Duration.ofMinutes(1));

        DataStream<OrderEvent> orderStream = env
                .fromSource(kafkaSource, watermarkStrategy, "kafka-orders-source")
                .uid("kafka-orders-source")
                .name("Kafka Orders Source");

        LOG.info("Kafka source configured: bootstrap={}, topic={}, group={}",
                kafkaBootstrap, kafkaTopic, consumerGroup);

        // ---------------------------------------------------------------
        // Apply CEP patterns
        // ---------------------------------------------------------------

        // Pattern 1: Fraud detection (multi-address orders)
        DataStream<FraudAlert> fraudAlerts = FraudDetectionPattern.apply(orderStream)
                .uid("fraud-detection-cep")
                .name("Fraud Detection CEP");

        // Pattern 2: Cancellation anomaly (spike detection)
        DataStream<CancellationAnomalyAlert> cancellationAlerts =
                OrderAnomalyPattern.apply(orderStream)
                        .uid("cancellation-anomaly-cep")
                        .name("Cancellation Anomaly CEP");

        // Log alert counts as side output for monitoring
        fraudAlerts.map(alert -> {
            LOG.info("FRAUD_ALERT: {}", alert);
            return alert;
        }).uid("fraud-alert-logger").name("Fraud Alert Logger");

        cancellationAlerts.map(alert -> {
            LOG.info("CANCELLATION_ANOMALY: {}", alert);
            return alert;
        }).uid("cancellation-alert-logger").name("Cancellation Alert Logger");

        // ---------------------------------------------------------------
        // Iceberg sinks
        // ---------------------------------------------------------------
        IcebergSink icebergSink = new IcebergSink(s3Bucket);
        icebergSink.setupAndWrite(tableEnv, fraudAlerts, cancellationAlerts);

        LOG.info("Iceberg sinks configured: warehouse=s3://{}/pipeline1-batch-etl/iceberg", s3Bucket);

        // ---------------------------------------------------------------
        // Execute
        // ---------------------------------------------------------------
        env.execute("Zomato Pipeline 1 - Batch CEP Job");
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private static String getConfig(String key, String defaultValue) {
        // Check system properties first, then environment variables
        String sysProp = System.getProperty(key);
        if (sysProp != null && !sysProp.isEmpty()) {
            return sysProp;
        }
        String envKey = key.toUpperCase().replace('.', '_');
        String envVal = System.getenv(envKey);
        if (envVal != null && !envVal.isEmpty()) {
            return envVal;
        }
        return defaultValue;
    }

    // -----------------------------------------------------------------------
    // Deserializer
    // -----------------------------------------------------------------------

    /**
     * JSON deserializer for {@link OrderEvent} used by the Kafka source.
     */
    private static class OrderEventDeserializer implements DeserializationSchema<OrderEvent> {

        private static final long serialVersionUID = 1L;
        private transient ObjectMapper objectMapper;

        @Override
        public void open(InitializationContext context) {
            this.objectMapper = new ObjectMapper();
        }

        @Override
        public OrderEvent deserialize(byte[] message) throws IOException {
            if (objectMapper == null) {
                objectMapper = new ObjectMapper();
            }
            return objectMapper.readValue(message, OrderEvent.class);
        }

        @Override
        public boolean isEndOfStream(OrderEvent nextElement) {
            return false;
        }

        @Override
        public TypeInformation<OrderEvent> getProducedType() {
            return TypeInformation.of(OrderEvent.class);
        }
    }
}
