package com.bank.database;

import com.bank.model.Transaction;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Sorts;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Repository layer for CRUD operations on MongoDB 'bank.transactions' collection.
 * Supports Phase 8 (storage), Phase 11 (retrieval & search), and Phase 14 (deduplication).
 */
public class TransactionRepository {

    private static final Logger logger = LoggerFactory.getLogger(TransactionRepository.class);

    private final MongoCollection<Document> collection;

    public TransactionRepository(MongoDBConnection connection) {
        this.collection = connection.getCollection();
        ensureIndexes();
    }

    /**
     * Creates indexes on transactionId (unique), timestamp, and user fields.
     */
    private void ensureIndexes() {
        try {
            collection.createIndex(Indexes.ascending("transactionId"), new IndexOptions().unique(true));
            collection.createIndex(Indexes.descending("timestamp"));
            collection.createIndex(Indexes.ascending("senderId"));
            collection.createIndex(Indexes.ascending("receiverId"));
        } catch (Exception e) {
            logger.warn("Index creation warning (may already exist): {}", e.getMessage());
        }
    }

    /**
     * Saves a Transaction to MongoDB. Returns true if saved, false if duplicate or failed.
     */
    public boolean save(Transaction txn) {
        try {
            Document doc = toDocument(txn);
            collection.insertOne(doc);
            logger.info("Saved transaction to MongoDB: {}", txn.getTransactionId());
            return true;
        } catch (Exception e) {
            if (e.getMessage() != null && e.getMessage().contains("duplicate key")) {
                logger.warn("Duplicate transaction ignored in MongoDB: {}", txn.getTransactionId());
            } else {
                logger.error("Failed to save transaction {}: {}", txn.getTransactionId(), e.getMessage());
            }
            return false;
        }
    }

    /**
     * Checks if a transaction already exists in MongoDB.
     */
    public boolean exists(String transactionId) {
        if (transactionId == null) return false;
        return collection.countDocuments(Filters.eq("transactionId", transactionId)) > 0;
    }

    /**
     * Find transaction by ID.
     */
    public Transaction findById(String transactionId) {
        Document doc = collection.find(Filters.eq("transactionId", transactionId)).first();
        return doc != null ? toTransaction(doc) : null;
    }

    /**
     * Find all transactions up to limit, sorted newest first.
     */
    public List<Transaction> findAll(int limit) {
        List<Transaction> list = new ArrayList<>();
        FindIterable<Document> docs = collection.find()
                .sort(Sorts.descending("timestamp"))
                .limit(limit);
        for (Document doc : docs) {
            list.add(toTransaction(doc));
        }
        return list;
    }

    /**
     * Find transactions involving a specific user (as sender or receiver).
     */
    public List<Transaction> findByUser(String userId, int limit) {
        List<Transaction> list = new ArrayList<>();
        Bson filter = Filters.or(
                Filters.eq("senderId", userId),
                Filters.eq("receiverId", userId)
        );
        FindIterable<Document> docs = collection.find(filter)
                .sort(Sorts.descending("timestamp"))
                .limit(limit);
        for (Document doc : docs) {
            list.add(toTransaction(doc));
        }
        return list;
    }

    /**
     * Filter by transaction type.
     */
    public List<Transaction> findByType(String transactionType, int limit) {
        List<Transaction> list = new ArrayList<>();
        FindIterable<Document> docs = collection.find(Filters.eq("transactionType", transactionType))
                .sort(Sorts.descending("timestamp"))
                .limit(limit);
        for (Document doc : docs) {
            list.add(toTransaction(doc));
        }
        return list;
    }

    /**
     * Total number of transactions in the database.
     */
    public long count() {
        return collection.countDocuments();
    }

    // Helper converters
    public static Document toDocument(Transaction txn) {
        Document doc = new Document();
        doc.append("transactionId", txn.getTransactionId());
        doc.append("senderId", txn.getSenderId());
        doc.append("receiverId", txn.getReceiverId());
        doc.append("amount", txn.getAmount());
        doc.append("transactionType", txn.getTransactionType());
        doc.append("status", txn.getStatus());
        doc.append("timestamp", txn.getTimestamp());
        return doc;
    }

    public static Transaction toTransaction(Document doc) {
        Transaction txn = new Transaction();
        txn.setTransactionId(doc.getString("transactionId"));
        txn.setSenderId(doc.getString("senderId"));
        txn.setReceiverId(doc.getString("receiverId"));
        Object amt = doc.get("amount");
        if (amt instanceof Number) {
            txn.setAmount(((Number) amt).doubleValue());
        }
        txn.setTransactionType(doc.getString("transactionType"));
        txn.setStatus(doc.getString("status"));
        txn.setTimestamp(doc.getString("timestamp"));
        return txn;
    }
}
