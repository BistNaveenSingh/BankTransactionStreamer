package com.bank.consumer;

import com.bank.model.Transaction;
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
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.DoubleAdder;

/**
 * Secondary Kafka Consumer component (Phase 16).
 * Belongs to an independent consumer group ("bank-analytics-group")
 * and computes real-time streaming metrics without touching MongoDB.
 */
public class AnalyticsConsumer implements Runnable, AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(AnalyticsConsumer.class);

    public static final String DEFAULT_GROUP_ID = "bank-analytics-group";

    private final String bootstrapServers;
    private final String topic;
    private final String groupId;
    private final ObjectMapper objectMapper;

    private final AtomicBoolean running = new AtomicBoolean(false);

    // Streaming Analytics State
    private final AtomicLong totalTransactions = new AtomicLong(0);
    private final DoubleAdder totalAmount = new DoubleAdder();
    private final AtomicLong debitCount = new AtomicLong(0);
    private final AtomicLong creditCount = new AtomicLong(0);
    private final AtomicLong upiCount = new AtomicLong(0);

    private KafkaConsumer<String, String> consumer;
    private Thread workerThread;

    public AnalyticsConsumer(String bootstrapServers, String topic) {
        this(bootstrapServers, topic, DEFAULT_GROUP_ID);
    }

    public AnalyticsConsumer(String bootstrapServers, String topic, String groupId) {
        this.bootstrapServers = bootstrapServers;
        this.topic = topic;
        this.groupId = groupId;
        this.objectMapper = new ObjectMapper();
    }

    public synchronized void start() {
        if (running.get()) return;
        running.set(true);
        workerThread = new Thread(this, "Kafka-Analytics-Consumer-Thread");
        workerThread.start();
        logger.info("Started AnalyticsConsumer worker thread for topic '{}' with group '{}'", topic, groupId);
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

        try {
            consumer = new KafkaConsumer<>(props);
            consumer.subscribe(Collections.singletonList(topic));
            logger.info("AnalyticsConsumer subscribed to topic '{}'", topic);

            while (running.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> record : records) {
                    try {
                        Transaction txn = objectMapper.readValue(record.value(), Transaction.class);
                        totalTransactions.incrementAndGet();
                        totalAmount.add(txn.getAmount());

                        String type = txn.getTransactionType() != null ? txn.getTransactionType().toUpperCase() : "";
                        if (type.contains("DEBIT")) {
                            debitCount.incrementAndGet();
                        } else if (type.contains("CREDIT")) {
                            creditCount.incrementAndGet();
                        } else if (type.contains("UPI")) {
                            upiCount.incrementAndGet();
                        }
                    } catch (Exception e) {
                        logger.error("Analytics parsing error: {}", e.getMessage());
                    }
                }
            }
        } catch (WakeupException e) {
            if (running.get()) throw e;
        } catch (Exception e) {
            logger.error("Error in AnalyticsConsumer loop: {}", e.getMessage());
        } finally {
            try {
                if (consumer != null) consumer.close();
            } catch (Exception ignored) {}
        }
    }

    public Map<String, Object> getMetrics() {
        long count = totalTransactions.get();
        double sum = totalAmount.sum();
        double avg = count > 0 ? (sum / count) : 0.0;

        Map<String, Object> map = new HashMap<>();
        map.put("totalTransactions", count);
        map.put("totalAmount", Math.round(sum * 100.0) / 100.0);
        map.put("averageAmount", Math.round(avg * 100.0) / 100.0);
        map.put("debitTransactions", debitCount.get());
        map.put("creditTransactions", creditCount.get());
        map.put("upiTransactions", upiCount.get());
        return map;
    }

    public synchronized void stop() {
        if (!running.get()) return;
        running.set(false);
        if (consumer != null) consumer.wakeup();
        if (workerThread != null) {
            try {
                workerThread.join(3000);
            } catch (InterruptedException ignored) {}
        }
    }

    @Override
    public void close() {
        stop();
    }
}
