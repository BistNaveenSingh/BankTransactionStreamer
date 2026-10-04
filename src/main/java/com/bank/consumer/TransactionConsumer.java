package com.bank.consumer;

import com.bank.database.TransactionRepository;
import com.bank.model.Transaction;
import com.bank.model.TransactionValidator;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Kafka Consumer component (Phase 7 & Phase 8).
 * Reads streamed transaction events from "bank-transactions" and persists them to MongoDB.
 */
public class TransactionConsumer implements Runnable, AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(TransactionConsumer.class);

    public static final String DEFAULT_GROUP_ID = "bank-transaction-consumers";

    private final String bootstrapServers;
    private final String topic;
    private final String groupId;
    private final TransactionRepository repository;
    private final ObjectMapper objectMapper;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicLong processedCount = new AtomicLong(0);
    private final AtomicLong errorCount = new AtomicLong(0);
    private final AtomicLong duplicateCount = new AtomicLong(0);

    private KafkaConsumer<String, String> consumer;
    private Thread workerThread;

    public TransactionConsumer(String bootstrapServers, String topic, String groupId, TransactionRepository repository) {
        this.bootstrapServers = bootstrapServers;
        this.topic = topic;
        this.groupId = groupId;
        this.repository = repository;
        this.objectMapper = new ObjectMapper();
    }

    public synchronized void start() {
        if (running.get()) {
            logger.warn("TransactionConsumer is already running.");
            return;
        }

        running.set(true);
        workerThread = new Thread(this, "Kafka-Transaction-Consumer-Thread");
        workerThread.start();
        logger.info("Started TransactionConsumer worker thread for topic '{}' with group '{}'", topic, groupId);
    }

    @Override
    public void run() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");

        try {
            consumer = new KafkaConsumer<>(props);
            consumer.subscribe(Collections.singletonList(topic));
            logger.info("Kafka Consumer subscribed to topic '{}'", topic);

            while (running.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> record : records) {
                    processRecord(record);
                }
            }
        } catch (WakeupException e) {
            // Normal shutdown via consumer.wakeup()
            if (running.get()) throw e;
        } catch (Exception e) {
            logger.error("Unexpected error in Consumer loop: {}", e.getMessage(), e);
        } finally {
            try {
                if (consumer != null) {
                    consumer.close();
                    logger.info("Kafka Consumer closed.");
                }
            } catch (Exception e) {
                logger.error("Error closing Kafka Consumer: {}", e.getMessage());
            }
        }
    }

    private void processRecord(ConsumerRecord<String, String> record) {
        try {
            String json = record.value();
            logger.info("Received event from partition {} at offset {}: {}",
                    record.partition(), record.offset(), json);

            Transaction txn = objectMapper.readValue(json, Transaction.class);

            // Validation
            TransactionValidator.ValidationResult val = TransactionValidator.validate(txn);
            if (!val.isValid()) {
                logger.warn("Discarding invalid transaction {}: {}", txn.getTransactionId(), val.getMessage());
                errorCount.incrementAndGet();
                return;
            }

            // Deduplication check
            if (repository.exists(txn.getTransactionId())) {
                logger.warn("Transaction {} already exists in MongoDB (Duplicate). Skipping.", txn.getTransactionId());
                duplicateCount.incrementAndGet();
                return;
            }

            // Persist to MongoDB
            boolean saved = repository.save(txn);
            if (saved) {
                processedCount.incrementAndGet();
                logger.info("Transaction {} successfully committed to MongoDB.", txn.getTransactionId());
            } else {
                errorCount.incrementAndGet();
            }
        } catch (Exception e) {
            logger.error("Failed to parse/process record payload: {}", e.getMessage());
            errorCount.incrementAndGet();
        }
    }

    public synchronized void stop() {
        if (!running.get()) return;
        running.set(false);
        if (consumer != null) {
            consumer.wakeup();
        }
        if (workerThread != null) {
            try {
                workerThread.join(3000);
            } catch (InterruptedException ignored) {}
        }
        logger.info("TransactionConsumer stopped.");
    }

    public boolean isRunning() {
        return running.get();
    }

    public long getProcessedCount() {
        return processedCount.get();
    }

    public long getErrorCount() {
        return errorCount.get();
    }

    public long getDuplicateCount() {
        return duplicateCount.get();
    }

    @Override
    public void close() {
        stop();
    }
}
