package com.bank.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Objects;
import java.util.UUID;

/**
 * Represents a digital payment/UPI bank transaction event.
 * Maps between Java objects, Kafka JSON message payloads, and MongoDB documents.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Transaction {

    @JsonProperty("transactionId")
    private String transactionId;

    @JsonProperty("senderId")
    private String senderId;

    @JsonProperty("receiverId")
    private String receiverId;

    @JsonProperty("amount")
    private double amount;

    @JsonProperty("transactionType")
    private String transactionType; // e.g., UPI_PAYMENT, NEFT, IMPS, DEBIT, CREDIT

    @JsonProperty("status")
    private String status; // e.g., INITIATED, SUCCESS, FAILED

    @JsonProperty("timestamp")
    private String timestamp; // ISO-8601 representation

    public Transaction() {
        // Default constructor required for Jackson JSON deserialization
    }

    public Transaction(String transactionId, String senderId, String receiverId, double amount, String transactionType, String status, String timestamp) {
        this.transactionId = transactionId;
        this.senderId = senderId;
        this.receiverId = receiverId;
        this.amount = amount;
        this.transactionType = transactionType;
        this.status = status;
        this.timestamp = timestamp;
    }

    /**
     * Factory helper to create a new transaction with generated ID and current timestamp.
     */
    public static Transaction create(String senderId, String receiverId, double amount, String transactionType) {
        String txnId = "TXN" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();
        String now = LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
        return new Transaction(txnId, senderId, receiverId, amount, transactionType, "SUCCESS", now);
    }

    // Getters and Setters
    public String getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }

    public String getSenderId() {
        return senderId;
    }

    public void setSenderId(String senderId) {
        this.senderId = senderId;
    }

    public String getReceiverId() {
        return receiverId;
    }

    public void setReceiverId(String receiverId) {
        this.receiverId = receiverId;
    }

    public double getAmount() {
        return amount;
    }

    public void setAmount(double amount) {
        this.amount = amount;
    }

    public String getTransactionType() {
        return transactionType;
    }

    public void setTransactionType(String transactionType) {
        this.transactionType = transactionType;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Transaction that = (Transaction) o;
        return Double.compare(that.amount, amount) == 0 &&
                Objects.equals(transactionId, that.transactionId) &&
                Objects.equals(senderId, that.senderId) &&
                Objects.equals(receiverId, that.receiverId) &&
                Objects.equals(transactionType, that.transactionType) &&
                Objects.equals(status, that.status) &&
                Objects.equals(timestamp, that.timestamp);
    }

    @Override
    public int hashCode() {
        return Objects.hash(transactionId, senderId, receiverId, amount, transactionType, status, timestamp);
    }

    @Override
    public String toString() {
        return "Transaction{" +
                "transactionId='" + transactionId + '\'' +
                ", senderId='" + senderId + '\'' +
                ", receiverId='" + receiverId + '\'' +
                ", amount=" + amount +
                ", transactionType='" + transactionType + '\'' +
                ", status='" + status + '\'' +
                ", timestamp='" + timestamp + '\'' +
                '}';
    }
}
