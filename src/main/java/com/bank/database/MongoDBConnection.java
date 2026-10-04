package com.bank.database;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages the MongoDB Client and database connection for the 'bank' database.
 */
public class MongoDBConnection {

    private static final Logger logger = LoggerFactory.getLogger(MongoDBConnection.class);

    public static final String DEFAULT_CONNECTION_STRING = "mongodb://localhost:27017";
    public static final String DATABASE_NAME = "bank";
    public static final String COLLECTION_NAME = "transactions";

    private final MongoClient mongoClient;
    private final MongoDatabase database;
    private final MongoCollection<Document> collection;

    public MongoDBConnection() {
        this(DEFAULT_CONNECTION_STRING);
    }

    public MongoDBConnection(String connectionString) {
        logger.info("Connecting to MongoDB at: {}", connectionString);
        this.mongoClient = MongoClients.create(connectionString);
        this.database = mongoClient.getDatabase(DATABASE_NAME);
        this.collection = database.getCollection(COLLECTION_NAME);
    }

    /**
     * Pings the MongoDB server to verify connectivity.
     * @return true if ping succeeds, false otherwise.
     */
    public boolean ping() {
        try {
            Document pingResult = database.runCommand(new Document("ping", 1));
            Object okVal = pingResult.get("ok");
            if (okVal instanceof Number) {
                return ((Number) okVal).doubleValue() == 1.0;
            }
            return false;
        } catch (Exception e) {
            logger.error("MongoDB ping failed: {}", e.getMessage());
            return false;
        }
    }

    public MongoCollection<Document> getCollection() {
        return collection;
    }

    public MongoDatabase getDatabase() {
        return database;
    }

    public MongoClient getMongoClient() {
        return mongoClient;
    }

    public void close() {
        try {
            if (mongoClient != null) {
                mongoClient.close();
                logger.info("MongoDB client closed successfully.");
            }
        } catch (Exception e) {
            logger.error("Error closing MongoDB client: {}", e.getMessage());
        }
    }
}
