package com.bank;

import com.bank.model.Transaction;
import com.bank.model.TransactionValidator;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class TransactionValidatorTest {

    @Test
    @DisplayName("Valid transaction should pass validation")
    void testValidTransaction() {
        Transaction txn = Transaction.create("USER101", "USER205", 500.0, "UPI_PAYMENT");
        TransactionValidator.ValidationResult result = TransactionValidator.validate(txn);
        assertTrue(result.isValid(), "Valid transaction should pass");
    }

    @Test
    @DisplayName("Amount <= 0 should fail validation")
    void testZeroOrNegativeAmount() {
        Transaction txnZero = Transaction.create("USER101", "USER205", 0.0, "UPI_PAYMENT");
        TransactionValidator.ValidationResult r1 = TransactionValidator.validate(txnZero);
        assertFalse(r1.isValid(), "Zero amount should fail");
        assertTrue(r1.getMessage().contains("greater than 0"));

        Transaction txnNeg = Transaction.create("USER101", "USER205", -100.0, "UPI_PAYMENT");
        TransactionValidator.ValidationResult r2 = TransactionValidator.validate(txnNeg);
        assertFalse(r2.isValid(), "Negative amount should fail");
    }

    @Test
    @DisplayName("Empty sender or receiver ID should fail validation")
    void testEmptySenderOrReceiver() {
        Transaction txnNoSender = Transaction.create("", "USER205", 500.0, "UPI_PAYMENT");
        assertFalse(TransactionValidator.validate(txnNoSender).isValid(), "Empty sender should fail");

        Transaction txnNoReceiver = Transaction.create("USER101", "   ", 500.0, "UPI_PAYMENT");
        assertFalse(TransactionValidator.validate(txnNoReceiver).isValid(), "Blank receiver should fail");
    }

    @Test
    @DisplayName("Sender and Receiver cannot be identical")
    void testSameSenderAndReceiver() {
        Transaction txnSame = Transaction.create("USER101", "USER101", 500.0, "UPI_PAYMENT");
        TransactionValidator.ValidationResult result = TransactionValidator.validate(txnSame);
        assertFalse(result.isValid(), "Same sender and receiver must fail");
        assertTrue(result.getMessage().contains("cannot be the same"));
    }

    @Test
    @DisplayName("Invalid transaction type should fail validation")
    void testInvalidTransactionType() {
        Transaction txnInvalid = Transaction.create("USER101", "USER205", 500.0, "BITCOIN_PAYMENT");
        TransactionValidator.ValidationResult result = TransactionValidator.validate(txnInvalid);
        assertFalse(result.isValid(), "Unsupported payment type should fail");
        assertTrue(result.getMessage().contains("Invalid transaction type"));
    }
}
