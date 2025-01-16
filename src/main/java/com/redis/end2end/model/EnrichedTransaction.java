package com.redis.end2end.model;

import java.util.Objects;

public class EnrichedTransaction {

  private static final String DATA_FIELD = "data";
  public long accountId;
  public long timestamp;
  public long amount;
  public long transactionFee;

  public EnrichedTransaction() {
  }

  public EnrichedTransaction(long accountId, long timestamp, long amount, long transactionFee) {
    this.accountId = accountId;
    this.timestamp = timestamp;
    this.amount = amount;
    this.transactionFee = transactionFee;
  }

  public EnrichedTransaction(Transaction transaction, long transactionFee) {
    this.accountId = transaction.accountId;
    this.timestamp = transaction.timestamp;
    this.amount = transaction.amount;
    this.transactionFee = transactionFee;
  }

  public long getAccountId() {
    return accountId;
  }

  public void setAccountId(long accountId) {
    this.accountId = accountId;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  public long getAmount() {
    return amount;
  }

  public void setAmount(long amount) {
    this.amount = amount;
  }

  public long getTransactionFee() {
    return transactionFee;
  }

  public void setTransactionFee(long transactionFee) {
    this.transactionFee = transactionFee;
  }

  @Override
  public String toString() {
    return "EnrichedTransaction{" +
        "accountId=" + accountId +
        ", timestamp=" + timestamp +
        ", amount=" + amount +
        ", transactionFee=" + transactionFee +
        '}';
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    EnrichedTransaction that = (EnrichedTransaction) o;
    return accountId == that.accountId && timestamp == that.timestamp && amount == that.amount
        && transactionFee == that.transactionFee;
  }

  @Override
  public int hashCode() {
    return Objects.hash(accountId, timestamp, amount, transactionFee);
  }
}
