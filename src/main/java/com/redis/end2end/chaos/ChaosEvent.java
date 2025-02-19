package com.redis.end2end.chaos;

import java.time.Instant;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

@AllArgsConstructor
@NoArgsConstructor
@ToString(includeFieldNames = true)
@Data
public class ChaosEvent {
  long id;
  //Instant timestamp;
  long timestamp;
  String payload;
}
