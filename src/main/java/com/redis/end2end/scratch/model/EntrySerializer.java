package com.redis.end2end.scratch.model;

import com.redis.flink.RedisMessage;
import com.redis.flink.sink.RedisSerializer;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.formats.json.JsonSerializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

public class EntrySerializer implements RedisSerializer<Entry> {

  private static final String DATA_FIELD = "data";

  public static Entry of(RedisMessage message) throws IOException {
    String data = message.getData().get(DATA_FIELD);
    if (data != null) {
      try {
        return new ObjectMapper().readValue(data, Entry.class);
      } catch (Exception e) {
        throw new IOException("Failed to deserialize Entry from RedisMessage", e);
      }
    }
    throw new IOException(
        "Failed to deserialize Entry from RedisMessage " + DATA_FIELD + " field not found");
  }

  public static RedisMessage toRedisMessage(Entry entry) {
    JsonSerializationSchema<Entry> serializer = new JsonSerializationSchema<>(
        ObjectMapper::new);
    serializer.open(null);
    return RedisMessage.of(Long.toString(entry.getId()),
        new HashMap<>(Map.of(DATA_FIELD, new String(serializer.serialize(entry)))));
  }


  @Override
  public RedisMessage Serialize(Entry entry) {
    return EntrySerializer.toRedisMessage(entry);
  }

  @Override
  public RedisMessage Serialize(Entry entry, String key) {
    return EntrySerializer.toRedisMessage(entry);
  }
}