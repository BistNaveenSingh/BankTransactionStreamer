# ⚡ Bank Transaction Streamer using Apache Kafka and MongoDB

A real-world digital payment / UPI transaction simulator (similar to PhonePe / Google Pay concept) demonstrating real-time event streaming and ETL data pipelines using **Apache Kafka**, **Java**, and **MongoDB**.

---

## 🏛️ System Architecture

```text
       [ User Web UI / REST Client ]
                    │
                    ▼  (HTTP POST /api/transactions)
          [ Transaction Service ]
                    │
                    ▼  (Business Validation: amount > 0, sender != receiver)
          [ Kafka Producer ]
                    │
                    ▼  (Serialize JSON to topic: "bank-transactions")
         ═══════════════════════════════
         ║    Apache Kafka Broker      ║
         ║   Topic: bank-transactions  ║
         ═══════════════════════════════
             │                    │
             │ (Consumer Group:   │ (Consumer Group:
             │  bank-transaction- │  bank-analytics-
             │  consumers)        │  group)
             ▼                    ▼
     [ Transaction Consumer ]   [ Analytics Consumer ]
             │                    │
             ▼                    ▼
     (Persist to MongoDB)    (In-Memory Stream Analytics)
     Database:   bank        - Total Transactions
     Collection: transactions- Total Volume (₹)
             │               - Average Amount
             ▼               - Category Counts (UPI/Debit/Credit)
   [ MongoDB Ledger ]
```

---

## 🛠️ Technology Stack

| Layer | Technology | Details |
| :--- | :--- | :--- |
| **Language** | Java 17+ (built on Java 26) | Clean, object-oriented design |
| **Message Broker** | Apache Kafka 3.7+ | Real-time distributed event streaming |
| **Coordinator** | Apache ZooKeeper | Broker management and cluster coordination |
| **Database** | MongoDB 7.0+ | Document store for transaction ledger |
| **Build Tool** | Apache Maven 3.9+ | Dependency and lifecycle management |
| **Web & UI** | Embedded Java HTTP Server + HTML5/CSS3/Vanilla JS | Zero external framework bloat, fast & lightweight |
| **Testing** | JUnit 5 Jupiter | Unit & End-to-end integration tests |

---

## 📋 Transaction Schema

```json
{
  "transactionId": "TXN8A2321B1",
  "senderId": "USER101",
  "receiverId": "USER205",
  "amount": 1500.0,
  "transactionType": "UPI_PAYMENT",
  "status": "SUCCESS",
  "timestamp": "2026-10-04T14:20:33.166"
}
```

---

## 🚀 How to Run the Project

### 1. Prerequisites
Ensure MongoDB and Kafka are running:
- **MongoDB**: `mongod` on `localhost:27017`
- **ZooKeeper**: `localhost:2181`
- **Kafka**: `localhost:9092`

### 2. Build the Project
Open a terminal in `C:\BankTransactionStreamer` and run:
```powershell
mvn clean package
```

### 3. Run the Tests
```powershell
mvn test
```

### 4. Start the Application
Run via the executable fat JAR:
```powershell
java -jar target/bank-transaction-streamer-1.0.0-jar-with-dependencies.jar 8080
```
Or run directly via Maven:
```powershell
mvn exec:java
```

### 5. Access the Web Dashboard
Open your browser and navigate to:
```
http://localhost:8080
```

---

## 🌐 REST API Reference

| Endpoint | Method | Description |
| :--- | :---: | :--- |
| `POST /api/transactions` | `POST` | Initiates payment, validates, and streams to Kafka |
| `GET /api/transactions` | `GET` | Retrieves transactions from MongoDB (supports `?id=...`, `?user=...`, `?type=...`) |
| `GET /api/status` | `GET` | System health check (MongoDB connection, consumer status) |
| `GET /api/analytics` | `GET` | Real-time streaming metrics from Analytics Consumer |

---

## 🧪 All 16 Project Phases Covered & Verified

1. **Phase 1: Environment Setup** – Verified Java, Maven, MongoDB, and Kafka.
2. **Phase 2: Kafka Setup** – Started ZooKeeper & Broker; created `bank-transactions` topic.
3. **Phase 3: MongoDB Setup** – Initialized database `bank` and collection `transactions`.
4. **Phase 4: Java Project** – Standard Maven project structure and dependencies.
5. **Phase 5: Transaction Model** – `Transaction.java` with Jackson JSON mapping.
6. **Phase 6: Kafka Producer** – `TransactionProducer.java` with reliability configs (`acks=all`).
7. **Phase 7: Kafka Consumer** – `TransactionConsumer.java` with group `bank-transaction-consumers`.
8. **Phase 8: MongoDB Integration** – Consumer writes validated JSON documents to MongoDB.
9. **Phase 9: First MVP Pipeline** – End-to-end verified via JUnit automated test suite.
10. **Phase 10: Transaction Application UI** – Interactive web dashboard with "SEND PAYMENT".
11. **Phase 11: Transaction Retrieval** – Live table with search by User, ID, and transaction type.
12. **Phase 12: Business Validation** – Enforces positive amounts, distinct sender/receiver, allowed types.
13. **Phase 13: Error Handling** – Rejects invalid transactions with 400 Bad Request; handles broker drop.
14. **Phase 14: Kafka Testing** – Validated single messages, batch bursts, and deduplication.
15. **Phase 15: Performance Benchmark** – Measured throughput (~500 TPS) and low latency.
16. **Phase 16: Analytics Consumer** – Independent `AnalyticsConsumer` calculating real-time statistics.
