package com.bank;

import com.bank.model.Transaction;
import com.bank.service.TransactionService;
import org.junit.jupiter.api.*;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class PipelineIntegrationTest {

    private static TransactionService service;

    @BeforeAll
    static void setUp() {
        service = new TransactionService("localhost:9092", "bank-transactions", "mongodb://localhost:27017");
        // Allow consumers a moment to join Kafka group
        try { Thread.sleep(2000); } catch (InterruptedException ignored) {}
    }

    @AfterAll
    static void tearDown() {
        if (service != null) {
            service.close();
        }
    }

    @Test
    @Order(1)
    @DisplayName("Phase 9 MVP: Single transaction streaming from Producer through Kafka into MongoDB")
    void testSingleTransactionStreaming() throws Exception {
        String testSender = "TEST_ALICE_" + UUID.randomUUID().toString().substring(0, 4);
        String testReceiver = "TEST_BOB_" + UUID.randomUUID().toString().substring(0, 4);
        double amount = 750.50;

        Transaction txn = service.processPayment(testSender, testReceiver, amount, "UPI_PAYMENT");
        assertNotNull(txn.getTransactionId());

        // Wait for Kafka consumer to poll and commit to MongoDB
        boolean foundInDb = false;
        for (int i = 0; i < 20; i++) {
            Thread.sleep(500);
            Transaction stored = service.getTransactionById(txn.getTransactionId());
            if (stored != null) {
                foundInDb = true;
                assertEquals(testSender, stored.getSenderId());
                assertEquals(testReceiver, stored.getReceiverId());
                assertEquals(amount, stored.getAmount());
                assertEquals("UPI_PAYMENT", stored.getTransactionType());
                assertEquals("SUCCESS", stored.getStatus());
                break;
            }
        }

        assertTrue(foundInDb, "Transaction should be consumed from Kafka and saved into MongoDB within 10s");
    }

    @Test
    @Order(2)
    @DisplayName("Phase 14: Duplicate transaction rejection")
    void testDuplicateTransactionHandling() throws Exception {
        Transaction original = Transaction.create("DUP_USER1", "DUP_USER2", 120.0, "DEBIT");

        // Send once
        service.sendRawTransaction(original);
        Thread.sleep(2000);

        long countBefore = service.getTotalCount();

        // Send exact same transaction again (duplicate txnId)
        service.sendRawTransaction(original);
        Thread.sleep(2000);

        long countAfter = service.getTotalCount();
        assertEquals(countBefore, countAfter, "Duplicate transaction must not be saved twice in MongoDB");
    }

    @Test
    @Order(3)
    @DisplayName("Phase 14 & 15: Batch streaming & Performance metric measurement")
    void testBatchStreamingAndPerformance() throws Exception {
        int batchSize = 25;
        long startTime = System.currentTimeMillis();

        for (int i = 0; i < batchSize; i++) {
            String type = (i % 2 == 0) ? "UPI_PAYMENT" : "IMPS";
            service.processPayment("BATCH_SENDER_" + i, "BATCH_RECV_" + i, 100.0 + i, type);
        }

        long sendDuration = System.currentTimeMillis() - startTime;
        double sendTps = (batchSize / (double) sendDuration) * 1000.0;
        System.out.println("=== Phase 15 Performance Benchmark ===");
        System.out.printf("Sent %d transactions in %d ms (~%.2f TPS)%n", batchSize, sendDuration, sendTps);

        // Wait for all to be consumed
        Thread.sleep(3000);

        List<Transaction> all = service.getAllTransactions(50);
        assertFalse(all.isEmpty(), "Transactions should be retrievable from MongoDB");
    }

    @Test
    @Order(4)
    @DisplayName("Phase 16: Analytics consumer metrics validation")
    void testAnalyticsMetrics() {
        Map<String, Object> analytics = service.getAnalytics();
        assertNotNull(analytics);
        assertTrue(analytics.containsKey("totalTransactions"));
        assertTrue(analytics.containsKey("totalAmount"));
        assertTrue(analytics.containsKey("averageAmount"));

        long total = ((Number) analytics.get("totalTransactions")).longValue();
        assertTrue(total > 0, "Analytics consumer should have processed streaming events");
        System.out.println("Analytics Consumer Output: " + analytics);
    }
}
