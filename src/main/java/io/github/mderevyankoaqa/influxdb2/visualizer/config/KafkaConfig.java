package io.github.mderevyankoaqa.influxdb2.visualizer.config;

import com.influxdb.utils.Arguments;
import org.apache.jmeter.visualizers.backend.BackendListenerContext;

/**
 * Configuration for the Kafka writer.
 *
 * <p>The plugin serializes every JMeter sample to InfluxDB line protocol and
 * publishes it to a Kafka topic. Telegraf or another consumer is responsible
 * for writing the messages into InfluxDB.</p>
 */
public class KafkaConfig {

    public static final String DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092";
    public static final String DEFAULT_TOPIC = "jmeter-metrics";
    public static final String DEFAULT_ACKS = "all";
    public static final String DEFAULT_COMPRESSION_TYPE = "gzip";
    public static final int DEFAULT_LINGER_MS = 20;
    public static final int DEFAULT_BATCH_SIZE = 16384;
    public static final int DEFAULT_MAX_BLOCK_MS = 1000;
    public static final int DEFAULT_RETRIES = 3;
    public static final int DEFAULT_DELIVERY_TIMEOUT_MS = 30000;
    public static final int DEFAULT_THRESHOLD_ERROR = 5;
    public static final int DEFAULT_RESPONSE_BODY_LENGTH = 2000;

    public static final String KEY_BOOTSTRAP_SERVERS = "kafkaBootstrapServers";
    public static final String KEY_TOPIC = "kafkaTopic";
    public static final String KEY_ACKS = "kafkaAcks";
    public static final String KEY_COMPRESSION_TYPE = "kafkaCompressionType";
    public static final String KEY_LINGER_MS = "kafkaLingerMs";
    public static final String KEY_BATCH_SIZE = "kafkaBatchSize";
    public static final String KEY_MAX_BLOCK_MS = "kafkaMaxBlockMs";
    public static final String KEY_RETRIES = "kafkaRetries";
    public static final String KEY_DELIVERY_TIMEOUT_MS = "kafkaDeliveryTimeoutMs";
    public static final String KEY_THRESHOLD_ERROR = "kafkaThresholdError";
    public static final String KEY_RESPONSE_BODY_LENGTH = "responseBodyLength";

    private final String bootstrapServers;
    private final String topic;
    private final String acks;
    private final String compressionType;
    private final int lingerMs;
    private final int batchSize;
    private final int maxBlockMs;
    private final int retries;
    private final int deliveryTimeoutMs;
    private final int thresholdError;
    private final int responseBodyLength;

    /**
     * Creates a configuration from the backend listener context.
     *
     * @param context the JMeter backend listener context
     */
    public KafkaConfig(BackendListenerContext context) {
        this.bootstrapServers = context.getParameter(KEY_BOOTSTRAP_SERVERS, DEFAULT_BOOTSTRAP_SERVERS);
        Arguments.checkNonEmpty(this.bootstrapServers, KEY_BOOTSTRAP_SERVERS);

        this.topic = context.getParameter(KEY_TOPIC, DEFAULT_TOPIC);
        Arguments.checkNonEmpty(this.topic, KEY_TOPIC);

        this.acks = context.getParameter(KEY_ACKS, DEFAULT_ACKS);
        Arguments.checkNonEmpty(this.acks, KEY_ACKS);

        this.compressionType = context.getParameter(KEY_COMPRESSION_TYPE, DEFAULT_COMPRESSION_TYPE);
        Arguments.checkNonEmpty(this.compressionType, KEY_COMPRESSION_TYPE);

        this.lingerMs = context.getIntParameter(KEY_LINGER_MS, DEFAULT_LINGER_MS);
        Arguments.checkPositiveNumber(this.lingerMs, KEY_LINGER_MS);

        this.batchSize = context.getIntParameter(KEY_BATCH_SIZE, DEFAULT_BATCH_SIZE);
        Arguments.checkPositiveNumber(this.batchSize, KEY_BATCH_SIZE);

        this.maxBlockMs = context.getIntParameter(KEY_MAX_BLOCK_MS, DEFAULT_MAX_BLOCK_MS);
        Arguments.checkPositiveNumber(this.maxBlockMs, KEY_MAX_BLOCK_MS);

        this.retries = context.getIntParameter(KEY_RETRIES, DEFAULT_RETRIES);
        Arguments.checkNotNegativeNumber(this.retries, KEY_RETRIES);

        this.deliveryTimeoutMs = context.getIntParameter(KEY_DELIVERY_TIMEOUT_MS, DEFAULT_DELIVERY_TIMEOUT_MS);
        Arguments.checkPositiveNumber(this.deliveryTimeoutMs, KEY_DELIVERY_TIMEOUT_MS);

        this.thresholdError = context.getIntParameter(KEY_THRESHOLD_ERROR, DEFAULT_THRESHOLD_ERROR);
        Arguments.checkPositiveNumber(this.thresholdError, KEY_THRESHOLD_ERROR);

        this.responseBodyLength = context.getIntParameter(KEY_RESPONSE_BODY_LENGTH, DEFAULT_RESPONSE_BODY_LENGTH);
        Arguments.checkNotNegativeNumber(this.responseBodyLength, KEY_RESPONSE_BODY_LENGTH);
    }

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public String getTopic() {
        return topic;
    }

    public String getAcks() {
        return acks;
    }

    public String getCompressionType() {
        return compressionType;
    }

    public int getLingerMs() {
        return lingerMs;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public int getMaxBlockMs() {
        return maxBlockMs;
    }

    public int getRetries() {
        return retries;
    }

    public int getDeliveryTimeoutMs() {
        return deliveryTimeoutMs;
    }

    public int getThresholdError() {
        return thresholdError;
    }

    public int getResponseBodyLength() {
        return responseBodyLength;
    }
}
