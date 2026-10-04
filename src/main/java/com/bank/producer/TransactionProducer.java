package com.bank.producer;

import com.bank.model.Transaction;
import com.bank.model.TransactionValidator;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.Future;

/**
 * Kafka Producer component (Phase 6).
 * Publishes validated transaction events into the "bank-transactions" topic.
 */
public class TransactionProducer implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(TransactionProducer.class);

    public static final String DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092";
    public static final String DEFAULT_TOPIC = "bank-transactions";

    private final String topic;
    private final KafkaProducer<String, String> producer;
    private final ObjectMapper objectMapper;

    public TransactionProducer() {
        this(DEFAULT_BOOTSTRAP_SERVERS, DEFAULT_TOPIC);
    }

    public TransactionProducer(String bootstrapServers, String topic) {
        this.topic = topic;
        this.objectMapper = new ObjectMapper();

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Reliability settings for financial transactions
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 5000); // 5 second timeout if broker is unavailable

        logger.info("Initializing Kafka Producer for topic '{}' with servers '{}'", topic, bootstrapServers);
        this.producer = new KafkaProducer<>(props);
    }

    /**
     * Synchronously publishes a transaction event to Kafka after validation.
     * @return RecordMetadata on success.
     * @throws Exception if validation fails or Kafka broker cannot be reached.
     */
    public RecordMetadata send(Transaction txn) throws Exception {
        TransactionValidator.ValidationResult valResult = TransactionValidator.validate(txn);
        if (!valResult.isValid()) {
            throw new IllegalArgumentException("Transaction validation failed: " + valResult.getMessage());
        }

        String jsonPayload = objectMapper.writeValueAsString(txn);
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, txn.getTransactionId(), jsonPayload);

        logger.info("Publishing transaction event [{}] to Kafka...", txn.getTransactionId());
        RecordMetadata metadata = producer.send(record).get(); // Synchronous send
        logger.info("Published to topic '{}', partition {}, offset {}",
                metadata.topic(), metadata.partition(), metadata.offset());
        return metadata;
    }

    /**
     * Asynchronously publishes a transaction event with a Future return.
     */
    public Future<RecordMetadata> sendAsync(Transaction txn) throws Exception {
        TransactionValidator.ValidationResult valResult = TransactionValidator.validate(txn);
        if (!valResult.isValid()) {
            throw new IllegalArgumentException("Transaction validation failed: " + valResult.getMessage());
        }

        String jsonPayload = objectMapper.writeValueAsString(txn);
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, txn.getTransactionId(), jsonPayload);
        return producer.send(record);
    }

    public String getTopic() {
        return topic;
    }

    @Override
    public void close() {
        try {
            logger.info("Flushing and closing Kafka Producer...");
            producer.flush();
            producer.close();
            logger.info("Kafka Producer closed.");
        } catch (Exception e) {
            logger.error("Error closing Kafka Producer: {}", e.getMessage());
        }
    }
}
