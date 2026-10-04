package com.bank.web;

import com.bank.model.Transaction;
import com.bank.service.TransactionService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Built-in lightweight HTTP Web Server (Phase 10 & 11).
 * Serves the web UI and JSON REST endpoints without adding heavy frameworks.
 */
public class WebServer {

    private static final Logger logger = LoggerFactory.getLogger(WebServer.class);

    private final int port;
    private final TransactionService service;
    private final ObjectMapper objectMapper;
    private HttpServer server;

    public WebServer(int port, TransactionService service) {
        this.port = port;
        this.service = service;
        this.objectMapper = new ObjectMapper();
    }

    public void start() throws IOException {
        server = HttpServer.create(new InetSocketAddress(port), 0);

        server.createContext("/", new StaticFileHandler());
        server.createContext("/api/transactions", new TransactionsApiHandler());
        server.createContext("/api/status", new StatusApiHandler());
        server.createContext("/api/analytics", new AnalyticsApiHandler());

        server.setExecutor(null); // default executor
        server.start();
        logger.info("Web Application UI started at http://localhost:{}", port);
    }

    public void stop() {
        if (server != null) {
            server.stop(1);
            logger.info("Web Server stopped.");
        }
    }

    // Handler for Transaction creation and retrieval
    private class TransactionsApiHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.getResponseHeaders().add("Access-Control-Allow-Origin", "*");
            exchange.getResponseHeaders().add("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
            exchange.getResponseHeaders().add("Access-Control-Allow-Headers", "Content-Type");

            String method = exchange.getRequestMethod();

            if ("OPTIONS".equalsIgnoreCase(method)) {
                exchange.sendResponseHeaders(204, -1);
                return;
            }

            if ("POST".equalsIgnoreCase(method)) {
                try {
                    InputStream is = exchange.getRequestBody();
                    String body = new String(is.readAllBytes(), StandardCharsets.UTF_8);
                    Map<String, Object> req = objectMapper.readValue(body, Map.class);

                    String senderId = (String) req.get("senderId");
                    String receiverId = (String) req.get("receiverId");
                    double amount = Double.parseDouble(String.valueOf(req.get("amount")));
                    String type = (String) req.getOrDefault("transactionType", "UPI_PAYMENT");

                    Transaction txn = service.processPayment(senderId, receiverId, amount, type);

                    Map<String, Object> res = new HashMap<>();
                    res.put("success", true);
                    res.put("message", "Transaction sent to Kafka successfully");
                    res.put("transaction", txn);

                    sendJsonResponse(exchange, 200, res);
                } catch (IllegalArgumentException e) {
                    Map<String, Object> err = new HashMap<>();
                    err.put("success", false);
                    err.put("error", "Validation error: " + e.getMessage());
                    sendJsonResponse(exchange, 400, err);
                } catch (Exception e) {
                    Map<String, Object> err = new HashMap<>();
                    err.put("success", false);
                    err.put("error", "Processing error: " + e.getMessage());
                    sendJsonResponse(exchange, 500, err);
                }
            } else if ("GET".equalsIgnoreCase(method)) {
                try {
                    String query = exchange.getRequestURI().getQuery();
                    Map<String, String> params = parseQuery(query);

                    List<Transaction> list;
                    if (params.containsKey("id")) {
                        Transaction t = service.getTransactionById(params.get("id"));
                        list = t != null ? Collections.singletonList(t) : Collections.emptyList();
                    } else if (params.containsKey("user")) {
                        list = service.getTransactionsByUser(params.get("user"), 50);
                    } else if (params.containsKey("type")) {
                        list = service.getTransactionsByType(params.get("type"), 50);
                    } else {
                        list = service.getAllTransactions(50);
                    }

                    sendJsonResponse(exchange, 200, list);
                } catch (Exception e) {
                    Map<String, Object> err = new HashMap<>();
                    err.put("error", e.getMessage());
                    sendJsonResponse(exchange, 500, err);
                }
            } else {
                exchange.sendResponseHeaders(405, -1);
            }
        }
    }

    private class StatusApiHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.getResponseHeaders().add("Access-Control-Allow-Origin", "*");
            sendJsonResponse(exchange, 200, service.getSystemStatus());
        }
    }

    private class AnalyticsApiHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.getResponseHeaders().add("Access-Control-Allow-Origin", "*");
            sendJsonResponse(exchange, 200, service.getAnalytics());
        }
    }

    // Serves static files from /static/ folder on disk or classpath
    private class StaticFileHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            String path = exchange.getRequestURI().getPath();
            if (path == null || path.equals("/") || path.isEmpty()) {
                path = "/index.html";
            }

            File staticDir = new File("src/main/resources/static");
            if (!staticDir.exists()) {
                staticDir = new File("C:/BankTransactionStreamer/src/main/resources/static");
            }

            File file = new File(staticDir, path.replaceFirst("^/", ""));
            if (file.exists() && !file.isDirectory()) {
                String mime = getMimeType(file.getName());
                exchange.getResponseHeaders().set("Content-Type", mime);
                exchange.sendResponseHeaders(200, file.length());
                try (OutputStream os = exchange.getResponseBody(); FileInputStream fis = new FileInputStream(file)) {
                    fis.transferTo(os);
                }
            } else {
                String notFound = "404 Not Found: " + path;
                exchange.sendResponseHeaders(404, notFound.length());
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(notFound.getBytes(StandardCharsets.UTF_8));
                }
            }
        }
    }

    private void sendJsonResponse(HttpExchange exchange, int statusCode, Object data) throws IOException {
        byte[] bytes = objectMapper.writeValueAsBytes(data);
        exchange.sendResponseHeaders(statusCode, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }

    private Map<String, String> parseQuery(String query) {
        Map<String, String> map = new HashMap<>();
        if (query == null || query.isEmpty()) return map;
        for (String pair : query.split("&")) {
            String[] kv = pair.split("=");
            if (kv.length == 2) {
                map.put(URLDecoder.decode(kv[0], StandardCharsets.UTF_8),
                        URLDecoder.decode(kv[1], StandardCharsets.UTF_8));
            } else if (kv.length == 1) {
                map.put(URLDecoder.decode(kv[0], StandardCharsets.UTF_8), "");
            }
        }
        return map;
    }

    private String getMimeType(String filename) {
        if (filename.endsWith(".html")) return "text/html; charset=UTF-8";
        if (filename.endsWith(".css")) return "text/css; charset=UTF-8";
        if (filename.endsWith(".js")) return "application/javascript; charset=UTF-8";
        if (filename.endsWith(".json")) return "application/json";
        return "text/plain";
    }
}
