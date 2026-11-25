package io.github.mderevyankoaqa.influxdb2.visualizer.kafka.client;

import com.influxdb.client.write.Point;
import io.github.mderevyankoaqa.influxdb2.visualizer.config.KafkaConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Publishes JMeter samples to Kafka as InfluxDB line protocol messages.
 *
 * <p>The producer is asynchronous and idempotent. Back-pressure is bounded by
 * the Kafka producer buffer; when the configured error threshold is reached,
 * subsequent points are dropped until the load test ends.</p>
 */
public class KafkaMetricsClient {

    private static volatile KafkaMetricsClient instance;

    private final KafkaConfig config;
    private final Logger logger;
    private final KafkaProducer<String, String> producer;
    private final AtomicInteger consecutiveErrors = new AtomicInteger();
    private final AtomicLong lastDropWarnMs = new AtomicLong();

    private volatile String messageKey;
    private volatile boolean closed;

    /**
     * Creates the Kafka producer from the supplied configuration.
     *
     * @param config the Kafka writer configuration
     * @param logger the JMeter logger
     */
    private KafkaMetricsClient(KafkaConfig config, Logger logger) {
        this.config = config;
        this.logger = logger;
        this.messageKey = "jmeter";

        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBootstrapServers());
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, config.getCompressionType());
        properties.put(ProducerConfig.LINGER_MS_CONFIG, config.getLingerMs());
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, config.getBatchSize());
        properties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, config.getMaxBlockMs());
        properties.put(ProducerConfig.RETRIES_CONFIG, config.getRetries());
        properties.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, config.getDeliveryTimeoutMs());
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        String acks = config.getAcks();
        if (!"all".equalsIgnoreCase(acks)) {
            logger.warn("Idempotent Kafka producer requires acks=all, overriding configured value: " + acks);
            acks = "all";
        }
        properties.put(ProducerConfig.ACKS_CONFIG, acks);

        this.producer = new KafkaProducer<>(properties);
        logger.info("KafkaMetricsClient created, bootstrap servers --> " + config.getBootstrapServers()
                + ", topic --> " + config.getTopic() + ", compression --> " + config.getCompressionType());
    }

    /**
     * Returns the singleton Kafka writer.
     *
     * @param config the Kafka writer configuration
     * @param logger the JMeter logger
     * @return the singleton instance
     */
    public static KafkaMetricsClient getInstance(KafkaConfig config, Logger logger) {
        KafkaMetricsClient result = instance;
        if (result != null) {
            return result;
        }
        synchronized (KafkaMetricsClient.class) {
            if (instance == null) {
                instance = new KafkaMetricsClient(config, logger);
            }
            return instance;
        }
    }

    /**
     * Sets the Kafka message key used to preserve order within a run and node.
     *
     * @param key the message key
     */
    public void setMessageKey(String key) {
        if (key != null && !key.isEmpty()) {
            this.messageKey = key;
        }
    }

    /**
     * Serializes a point to line protocol and sends it to Kafka asynchronously.
     *
     * @param point the point to publish
     */
    public void send(Point point) {
        if (this.closed) {
            return;
        }

        int errors = this.consecutiveErrors.get();
        if (errors >= this.config.getThresholdError()) {
            long now = System.currentTimeMillis();
            long last = this.lastDropWarnMs.get();
            if (now - last > 10000 && this.lastDropWarnMs.compareAndSet(last, now)) {
                this.logger.warn("Kafka send is suspended after " + errors + " consecutive failures, points are dropped.");
            }
            return;
        }

        String lineProtocol = point.toLineProtocol();
        this.producer.send(new ProducerRecord<>(this.config.getTopic(), this.messageKey, lineProtocol), (metadata, exception) -> {
            if (exception == null) {
                this.consecutiveErrors.set(0);
                return;
            }

            int currentErrors = this.consecutiveErrors.incrementAndGet();
            if (currentErrors == this.config.getThresholdError() || currentErrors % 100 == 0) {
                this.logger.error("Failed to send metrics to Kafka, consecutive failures --> " + currentErrors
                        + ", reason --> " + exception.getMessage());
            }
        });
    }

    /**
     * Flushes pending records and closes the producer.
     */
    public void close() {
        if (this.closed) {
            return;
        }
        this.closed = true;
        try {
            this.producer.flush();
            this.producer.close(Duration.ofSeconds(10));
        } finally {
            instance = null;
        }
        this.logger.info("KafkaMetricsClient has been closed.");
    }
}
