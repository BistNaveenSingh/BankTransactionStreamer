package com.bank.service;

import com.bank.consumer.AnalyticsConsumer;
import com.bank.consumer.TransactionConsumer;
import com.bank.database.MongoDBConnection;
import com.bank.database.TransactionRepository;
import com.bank.model.Transaction;
import com.bank.model.TransactionValidator;
import com.bank.producer.TransactionProducer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Service orchestration layer combining Kafka Producer, Consumers, and MongoDB.
 */
public class TransactionService implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(TransactionService.class);

    private final MongoDBConnection mongoConnection;
    private final TransactionRepository repository;
    private final TransactionProducer producer;
    private final TransactionConsumer consumer;
    private final AnalyticsConsumer analyticsConsumer;

    public TransactionService() {
        this("localhost:9092", "bank-transactions", "mongodb://localhost:27017");
    }

    public TransactionService(String kafkaServers, String topic, String mongoUri) {
        logger.info("Initializing Bank Transaction Streamer Services...");

        this.mongoConnection = new MongoDBConnection(mongoUri);
        this.repository = new TransactionRepository(mongoConnection);

        this.producer = new TransactionProducer(kafkaServers, topic);
        this.consumer = new TransactionConsumer(kafkaServers, topic, TransactionConsumer.DEFAULT_GROUP_ID, repository);
        this.analyticsConsumer = new AnalyticsConsumer(kafkaServers, topic);

        // Start background consumers
        this.consumer.start();
        this.analyticsConsumer.start();
    }

    /**
     * Creates and streams a new payment transaction event.
     */
    public Transaction processPayment(String senderId, String receiverId, double amount, String type) throws Exception {
        Transaction txn = Transaction.create(senderId, receiverId, amount, type);

        // Validate
        TransactionValidator.ValidationResult val = TransactionValidator.validate(txn);
        if (!val.isValid()) {
            throw new IllegalArgumentException(val.getMessage());
        }

        // Send to Kafka
        RecordMetadata meta = producer.send(txn);
        logger.info("Transaction {} sent to Kafka partition {} offset {}",
                txn.getTransactionId(), meta.partition(), meta.offset());

        return txn;
    }

    /**
     * Publishes an existing Transaction object to Kafka.
     */
    public RecordMetadata sendRawTransaction(Transaction txn) throws Exception {
        return producer.send(txn);
    }

    // Retrieval methods (Phase 11)
    public List<Transaction> getAllTransactions(int limit) {
        return repository.findAll(limit);
    }

    public Transaction getTransactionById(String txnId) {
        return repository.findById(txnId);
    }

    public List<Transaction> getTransactionsByUser(String userId, int limit) {
        return repository.findByUser(userId, limit);
    }

    public List<Transaction> getTransactionsByType(String type, int limit) {
        return repository.findByType(type, limit);
    }

    public long getTotalCount() {
        return repository.count();
    }

    // Health and Status (Phase 13)
    public Map<String, Object> getSystemStatus() {
        Map<String, Object> status = new HashMap<>();
        boolean mongoOk = mongoConnection.ping();
        status.put("mongoConnected", mongoOk);
        status.put("consumerRunning", consumer.isRunning());
        status.put("consumerProcessedCount", consumer.getProcessedCount());
        status.put("consumerErrorCount", consumer.getErrorCount());
        status.put("consumerDuplicateCount", consumer.getDuplicateCount());
        status.put("totalTransactionsInDB", repository.count());
        status.put("kafkaTopic", producer.getTopic());
        return status;
    }

    // Analytics (Phase 16)
    public Map<String, Object> getAnalytics() {
        return analyticsConsumer.getMetrics();
    }

    public TransactionRepository getRepository() {
        return repository;
    }

    public MongoDBConnection getMongoConnection() {
        return mongoConnection;
    }

    @Override
    public void close() {
        try {
            consumer.stop();
            analyticsConsumer.stop();
            producer.close();
            mongoConnection.close();
            logger.info("TransactionService successfully shut down all resources.");
        } catch (Exception e) {
            logger.error("Error shutting down TransactionService: {}", e.getMessage());
        }
    }
}
