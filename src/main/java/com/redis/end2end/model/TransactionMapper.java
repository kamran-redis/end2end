package com.redis.end2end.model;

import com.redis.flink.RedisMessage;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.formats.json.JsonSerializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

public class TransactionMapper {

  private static final String DATA_FIELD = "data";

  public static Transaction of(RedisMessage message) throws IOException {
    String data = message.getData().get(DATA_FIELD);
    if (data != null) {
      try {
        return new ObjectMapper().readValue(data, Transaction.class);
      } catch (Exception e) {
        throw new IOException("Failed to deserialize Transaction from RedisMessage", e);
      }
    }
    throw new IOException(
        "Failed to deserialize Transaction from RedisMessage " + DATA_FIELD + " field not found");
  }

  public static RedisMessage toRedisMessage(Transaction transaction) {
    JsonSerializationSchema<Transaction> serializer = new JsonSerializationSchema<>(
        ObjectMapper::new);
    serializer.open(null);
    return RedisMessage.of(Long.toString(transaction.getAccountId()),
        new HashMap<>(Map.of(DATA_FIELD, new String(serializer.serialize(transaction)))));
  }
}