package com.bank;

import com.bank.service.TransactionService;
import com.bank.web.WebServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main application entrypoint.
 * Starts Kafka Producer, Consumer, Analytics Consumer, MongoDB connection, and the Web UI.
 */
public class Main {

    private static final Logger logger = LoggerFactory.getLogger(Main.class);
    private static final int DEFAULT_PORT = 8080;

    public static void main(String[] args) {
        printBanner();

        int port = DEFAULT_PORT;
        if (args.length > 0) {
            try {
                port = Integer.parseInt(args[0]);
            } catch (NumberFormatException ignored) {}
        }

        try {
            // 1. Initialize core streaming & database services
            logger.info("Starting Bank Transaction Streamer services...");
            TransactionService transactionService = new TransactionService(
                    "localhost:9092",
                    "bank-transactions",
                    "mongodb://localhost:27017"
            );

            // 2. Start Web Server and UI Dashboard
            WebServer webServer = new WebServer(port, transactionService);
            webServer.start();

            // 3. Register graceful shutdown hook
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                logger.info("Shutting down Bank Transaction Streamer...");
                webServer.stop();
                transactionService.close();
                logger.info("Application cleanly exited.");
            }, "Shutdown-Hook"));

            System.out.println("==================================================================");
            System.out.println("  BANK TRANSACTION STREAMER IS RUNNING!");
            System.out.println("  Web UI Dashboard: http://localhost:" + port);
            System.out.println("  Kafka Broker:     localhost:9092 (Topic: bank-transactions)");
            System.out.println("  MongoDB:          localhost:27017 (DB: bank, Collection: transactions)");
            System.out.println("==================================================================");

        } catch (Exception e) {
            logger.error("Failed to start Bank Transaction Streamer: {}", e.getMessage(), e);
            System.exit(1);
        }
    }

    private static void printBanner() {
        System.out.println("""
                 ____              _      _____                                 _   _             
                |  _ \\            | |    |_   _|                               | | (_)            
                | |_) | __ _ _ __ | | __   | |  _ __ __ _ _ __  ___  __ _  ___| |_ _  ___  _ __  
                |  _ < / _` | '_ \\| |/ /   | | | '__/ _` | '_ \\/ __|/ _` |/ __| __| |/ _ \\| '_ \\ 
                | |_) | (_| | | | |   <    | | | | | (_| | | | \\__ \\ (_| | (__| |_| | (_) | | | |
                |____/ \\__,_|_| |_|_|\\_\\   |_| |_|  \\__,_|_| |_|___/\\__,_|\\___|\\__|_|\\___/|_| |_|
                                                                                                  
                  [Apache Kafka & MongoDB Event Streaming Pipeline]
                """);
    }
}
