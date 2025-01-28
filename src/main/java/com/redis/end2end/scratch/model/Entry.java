package com.redis.end2end.scratch.model;

import java.util.Objects;

public class Entry {
  public long id;
  public long timestamp;

  public Entry() {
  }

  public Entry(long id, long timestamp) {
    this.id = id;
    this.timestamp = timestamp;
  }


  public long getId() {
    return id;
  }

  public void setId(long id) {
    this.id = id;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Entry entry = (Entry) o;
    return id == entry.id && timestamp == entry.timestamp;
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, timestamp);
  }

  @Override
  public String toString() {
    return "Entry{" +
        "id=" + id +
        ", timestamp=" + timestamp +
        '}';
  }


}
