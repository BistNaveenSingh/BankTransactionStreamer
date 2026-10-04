package com.bank.model;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Validates transaction business rules (Phase 12).
 * Ensures invalid transactions are never published to Kafka.
 */
public class TransactionValidator {

    private static final Set<String> ALLOWED_TYPES = new HashSet<>(Arrays.asList(
            "UPI_PAYMENT", "NEFT", "IMPS", "DEBIT", "CREDIT"
    ));

    public static class ValidationResult {
        private final boolean valid;
        private final String message;

        public ValidationResult(boolean valid, String message) {
            this.valid = valid;
            this.message = message;
        }

        public boolean isValid() {
            return valid;
        }

        public String getMessage() {
            return message;
        }

        public static ValidationResult success() {
            return new ValidationResult(true, "Validation passed");
        }

        public static ValidationResult fail(String message) {
            return new ValidationResult(false, message);
        }
    }

    public static ValidationResult validate(Transaction txn) {
        if (txn == null) {
            return ValidationResult.fail("Transaction cannot be null.");
        }

        if (txn.getSenderId() == null || txn.getSenderId().trim().isEmpty()) {
            return ValidationResult.fail("Sender ID cannot be empty.");
        }

        if (txn.getReceiverId() == null || txn.getReceiverId().trim().isEmpty()) {
            return ValidationResult.fail("Receiver ID cannot be empty.");
        }

        if (txn.getSenderId().trim().equalsIgnoreCase(txn.getReceiverId().trim())) {
            return ValidationResult.fail("Sender and Receiver cannot be the same user account.");
        }

        if (txn.getAmount() <= 0) {
            return ValidationResult.fail("Amount must be greater than 0. Received: " + txn.getAmount());
        }

        if (txn.getTransactionType() == null || txn.getTransactionType().trim().isEmpty()) {
            return ValidationResult.fail("Transaction type cannot be empty.");
        }

        String typeUpper = txn.getTransactionType().trim().toUpperCase();
        if (!ALLOWED_TYPES.contains(typeUpper)) {
            return ValidationResult.fail("Invalid transaction type: '" + txn.getTransactionType() +
                    "'. Allowed types: " + ALLOWED_TYPES);
        }

        return ValidationResult.success();
    }
}
