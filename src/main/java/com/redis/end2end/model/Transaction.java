package com.redis.end2end.model;

import com.redis.flink.RedisMessage;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.flink.formats.json.JsonSerializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

public class Transaction {

  private static final String DATA_FIELD = "data";
  public long accountId;
  public long timestamp;
  public long amount;

  public Transaction() {
  }

  public Transaction(long accountId, long timestamp, long amount) {
    this.accountId = accountId;
    this.timestamp = timestamp;
    this.amount = amount;
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

  @Override
  public String toString() {
    return "Transaction{" + "accountId=" + accountId + ", timestamp=" + timestamp + ", amount=" + amount + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Transaction transaction = (Transaction) o;
    return accountId == transaction.accountId && amount == transaction.amount
        && timestamp == transaction.timestamp;
  }

  @Override
  public int hashCode() {
    return Objects.hash(accountId, timestamp, amount);
  }



}
