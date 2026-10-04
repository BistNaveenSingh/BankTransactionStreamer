# Bank Transaction Streamer using Apache Kafka and MongoDB

A real-time digital payment and UPI transaction streaming pipeline built with Java, Apache Kafka, and MongoDB. Designed as an end-to-end laboratory and capstone demonstration of distributed event-driven architecture, producer-consumer decoupled pipelines, and persistent document storage.

---

## 1. Project Overview

Digital payment platforms (such as UPI, PhonePe, and Google Pay) process millions of financial events per minute. Handling such transaction volumes directly through traditional synchronous database writes introduces bottlenecks, tight coupling, and high latency.

This project demonstrates how **Apache Kafka** decouples the transaction submission layer from database persistence, ensuring high throughput, fault tolerance, and independent consumer scalability.

### Core Pipeline Flow

```text
[User Web UI / REST Client]
          │
          ▼  (HTTP POST /api/transactions)
[Transaction Service]
          │
          ▼  (Business Validation: amount > 0, sender != receiver)
[Kafka Producer]
          │
          ▼  (Topic: bank-transactions)
=====================================================
|                Apache Kafka Broker                |
|              Topic: bank-transactions             |
=====================================================
          │                                  │
          │ (Consumer Group:                 │ (Consumer Group:
          │  bank-transaction-consumers)     │  bank-analytics-group)
          ▼                                  ▼
[Transaction Consumer]             [Analytics Consumer]
          │                                  │
          ▼                                  ▼
(Persist to MongoDB)               (Real-Time In-Memory Metrics)
Database:   bank                   - Total Transactions
Collection: transactions           - Total Volume (INR)
          │                        - Average Amount
          ▼                        - UPI / Debit / Credit Counts
[MongoDB Stored Ledger]
```

---

## 2. Technology Stack

| Layer | Component | Version / Spec | Role |
| :--- | :--- | :--- | :--- |
| **Language** | Java | Java 17+ (JDK 26) | Core business logic and streaming clients |
| **Streaming Broker** | Apache Kafka | 3.7.0 | High-throughput distributed event broker |
| **Cluster Coordination** | Apache ZooKeeper | 3.8+ | Broker metadata and consensus management |
| **Database** | MongoDB | 7.0.14 | NoSQL document store for financial ledger |
| **Build & Dependencies** | Apache Maven | 3.9.16 | Dependency management and packaging |
| **Serialization** | Jackson Databind | 2.17.1 | JSON parsing and object serialization |
| **Web Server & UI** | Embedded Java HTTP Server | JDK Native | Lightweight REST API and web dashboard |
| **Testing** | JUnit 5 Jupiter | 5.10.2 | Unit and end-to-end integration tests |
| **Version Control** | Git / GitHub | Git 2.x | Source control repository |

---

## 3. Project Directory Structure

```text
BankTransactionStreamer/
├── pom.xml                                      # Maven build file with Kafka, MongoDB, Jackson dependencies
├── README.md                                    # Comprehensive project documentation
├── UI-SPEC.md                                   # UI design specification and styling tokens
├── .gitignore                                   # Git ignore rules for compiled classes and logs
└── src/
    ├── main/
    │   ├── java/com/bank/
    │   │   ├── Main.java                        # Application entrypoint with shutdown hooks
    │   │   ├── model/
    │   │   │   ├── Transaction.java             # Transaction POJO with JSON annotations
    │   │   │   └── TransactionValidator.java    # Business validation rules (Phase 12)
    │   │   ├── producer/
    │   │   │   └── TransactionProducer.java     # Kafka Producer (acks=all, retries=3)
    │   │   ├── consumer/
    │   │   │   ├── TransactionConsumer.java     # Kafka Consumer writing to MongoDB
    │   │   │   └── AnalyticsConsumer.java       # Phase 16 Real-time metrics consumer
    │   │   ├── database/
    │   │   │   ├── MongoDBConnection.java       # MongoDB connection lifecycle & ping
    │   │   │   └── TransactionRepository.java   # Queries, unique indexes, and deduplication
    │   │   ├── service/
    │   │   │   └── TransactionService.java      # Orchestrator coordinating all layers
    │   │   └── web/
    │   │       └── WebServer.java               # Built-in HTTP server for UI and REST API
    │   └── resources/
    │       └── static/
    │           ├── index.html                   # Responsive dashboard (no emojis, Material Symbols)
    │           ├── styles.css                   # Modern white/light fintech stylesheet
    │           └── app.js                       # Real-time event polling and dynamic UI logic
    └── test/java/com/bank/
        ├── TransactionValidatorTest.java        # Unit tests verifying validation rules
        └── PipelineIntegrationTest.java         # End-to-end automated test suite
```

---

## 4. Transaction Data Schema

Every payment event follows a structured JSON schema:

```json
{
  "transactionId": "TXN8A2321B1",
  "senderId": "USER101",
  "receiverId": "USER205",
  "amount": 1500.00,
  "transactionType": "UPI_PAYMENT",
  "status": "SUCCESS",
  "timestamp": "2026-10-04T14:20:33.166"
}
```

### Field Descriptions
- `transactionId`: Unique alphanumeric identifier (e.g., `TXN` prefix followed by UUID hash).
- `senderId`: Identifier of the originating account.
- `receiverId`: Identifier of the destination account.
- `amount`: Transaction value in INR (must be strictly greater than 0.00).
- `transactionType`: Payment channel: `UPI_PAYMENT`, `IMPS`, `NEFT`, `DEBIT`, `CREDIT`.
- `status`: Processing state (`SUCCESS`, `PENDING`, `FAILED`).
- `timestamp`: ISO-8601 formatted timestamp of transaction creation.

---

## 5. Implementation Phases (Phases 1 to 16)

This project was built and validated incrementally across 16 structured phases:

1. **Phase 1 - Environment Verification**: Verified Java 26, Maven 3.9, Kafka 3.7, and MongoDB 7.0 installations.
2. **Phase 2 - Kafka Cluster Setup**: Started ZooKeeper and Kafka Broker; created the `bank-transactions` topic with 1 partition.
3. **Phase 3 - MongoDB Initialization**: Created the `bank` database and `transactions` collection with unique indexes.
4. **Phase 4 - Java Maven Project Structure**: Formulated the standard Maven package layout and POM configuration.
5. **Phase 5 - Transaction Data Model**: Built `Transaction.java` with Jackson JSON annotations and factory helpers.
6. **Phase 6 - Kafka Producer**: Implemented `TransactionProducer.java` with acknowledgment safety (`acks=all`) and custom serializers.
7. **Phase 7 - Kafka Consumer**: Implemented `TransactionConsumer.java` utilizing consumer group `bank-transaction-consumers`.
8. **Phase 8 - MongoDB Persistence Integration**: Linked consumer poll loop to `TransactionRepository` for automatic database insertion.
9. **Phase 9 - First MVP Pipeline Test**: Verified complete automated event flow from producer to MongoDB with JUnit 5.
10. **Phase 10 - Web Dashboard UI**: Developed interactive web dashboard with payment submission and live pipeline stage visualizer.
11. **Phase 11 - Transaction Retrieval & Search**: Built filterable transaction ledger supporting lookups by ID, User, and Type.
12. **Phase 12 - Business Rule Validation**: Implemented strict validation rejecting zero/negative amounts, empty accounts, or identical sender/receiver.
13. **Phase 13 - Error Handling & Resilience**: Graceful handling of broker unavailability, network drops, and duplicate payloads.
14. **Phase 14 - Kafka Testing Scenarios**: Validated single events, burst traffic, and idempotency/duplicate rejection.
15. **Phase 15 - Performance Benchmark**: Measured throughput (~500 transactions per second) and sub-second end-to-end latency.
16. **Phase 16 - Stream Analytics Consumer**: Added an independent `AnalyticsConsumer` calculating real-time aggregated financial metrics.

---

## 6. How to Run and Demonstrate

### Step 1: Start MongoDB
Ensure MongoDB daemon is running on port 27017:
```powershell
mongod --dbpath "C:\data\db"
```
Or test via MongoDB shell:
```powershell
mongosh --eval "db.runCommand({ ping: 1 })"
```

### Step 2: Start Apache ZooKeeper
Open a PowerShell terminal and run:
```powershell
C:\kafka\bin\windows\zookeeper-server-start.bat C:\kafka\config\zookeeper.properties
```

### Step 3: Start Apache Kafka Broker
Open a second PowerShell terminal and run:
```powershell
C:\kafka\bin\windows\kafka-server-start.bat C:\kafka\config\server.properties
```

### Step 4: Build and Run the Application
In the project directory `C:\BankTransactionStreamer`:
```powershell
# Compile and run automated test suite
mvn clean test

# Package executable fat JAR
mvn clean package

# Start application server
java -jar target/bank-transaction-streamer-1.0.0-jar-with-dependencies.jar 8080
```

### Step 5: Open Web Dashboard
Navigate to `http://localhost:8080` in your browser.
- Fill in transaction details and click **SEND PAYMENT**.
- Observe the live 5-step pipeline illuminate as Kafka streams the record to MongoDB.
- Click **Send 5 Random** to demonstrate burst stream ingestion.
- Search and filter records in the stored ledger table.

---

## 7. REST API Documentation

| Endpoint | Method | Request Body | Description |
| :--- | :---: | :--- | :--- |
| `/api/transactions` | `POST` | `{"senderId":"U1","receiverId":"U2","amount":500,"transactionType":"UPI_PAYMENT"}` | Validates and streams transaction to Kafka |
| `/api/transactions` | `GET` | Query params: `?id=...`, `?user=...`, `?type=...` | Retrieves matching transactions from MongoDB |
| `/api/status` | `GET` | None | Returns broker health, MongoDB status, and consumer count |
| `/api/analytics` | `GET` | None | Returns aggregated stream statistics from Analytics Consumer |

---

## 8. College Laboratory & Viva Voce Q&A Reference

### Q1: Why use Apache Kafka instead of writing directly to MongoDB?
**Answer**: Writing directly from a frontend application to a database creates tight coupling. If the database experiences high load or becomes temporarily unavailable, payment submissions fail. Kafka acts as an ultra-high-throughput, durable buffer. The web app returns an immediate acknowledgment to the user, while consumers process and persist transactions at their own pace.

### Q2: What is the purpose of Consumer Groups in Kafka?
**Answer**: Consumer groups enable parallel processing and pub-sub architecture. In this project:
- Group 1 (`bank-transaction-consumers`) writes records to MongoDB.
- Group 2 (`bank-analytics-group`) reads the exact same topic independently to calculate real-time analytics without interfering with database persistence.

### Q3: What is the role of ZooKeeper in this architecture?
**Answer**: ZooKeeper maintains cluster metadata, tracks active broker nodes, elects the controller broker, and coordinates topic partitions and configuration changes.

### Q4: How is duplicate transaction handling (Idempotency) achieved?
**Answer**: MongoDB enforces a unique index on the `transactionId` field. When the consumer processes a duplicate message (e.g., during network retransmission), the repository detects the duplicate key and skips redundant insertion without throwing unhandled exceptions.

### Q5: What is the difference between Synchronous and Asynchronous send in Kafka?
**Answer**: Synchronous send blocks the calling thread until the broker acknowledges receipt (`producer.send(record).get()`), ensuring guaranteed delivery for financial transactions. Asynchronous send provides a callback, enabling non-blocking high-throughput writes.

---

## 9. Pushing this Project to GitHub

Follow these steps in your PowerShell terminal to upload the project to your GitHub account:

```powershell
# 1. Switch to project directory
cd C:\BankTransactionStreamer

# 2. Rename branch to main
git branch -M main

# 3. Add your remote GitHub repository URL
# (Replace USERNAME and REPO_NAME with your GitHub details)
git remote add origin https://github.com/USERNAME/REPO_NAME.git

# 4. Push all commits to GitHub
git push -u origin main
```
