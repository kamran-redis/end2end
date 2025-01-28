package com.redis.end2end.scratch;

import com.redis.end2end.scratch.model.Entry;
import com.redis.end2end.scratch.model.EntrySerializer;
import com.redis.flink.RedisMessage;
import com.redis.flink.source.partitioned.RedisSource;
import com.redis.flink.source.partitioned.RedisSourceBuilder;
import com.redis.flink.source.partitioned.RedisSourceConfig;
import com.redis.flink.source.partitioned.reader.deserializer.RedisMessageDeserializer;
import java.io.InputStream;
import java.util.Random;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.StreamEntryID;

public class SourceJobTest2 {

  public static void main(String[] args) throws Exception {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    ParameterTool argProperties = ParameterTool.fromArgs(args);
    ParameterTool config = null;

    try (InputStream stream = SourceJobTest2.class.getClassLoader()
        .getResourceAsStream("end2end.properties")) {
      config = ParameterTool.fromPropertiesFile(stream);
    }

    //to display
    env.getConfig().setGlobalJobParameters(config.mergeWith(argProperties));

    String jobName = argProperties.get("jobname", "NO_NAME_SOURCE" + new Random().nextInt(1000));

    String redisHost = config.get("redis.host");
    int redisPort = config.getInt("redis.port");
    String redisTopic = config.get("redis.topic");
    int redisTopicPartition = config.getInt("redis.topic.partitions");
    int checkpointInterval = config.getInt("flink.checkpointing.interval");
    int flinkBufferTimeout = config.getInt("flink.buffer.timeout");

    env.enableCheckpointing(checkpointInterval);
    env.setBufferTimeout(flinkBufferTimeout);
    env.setParallelism(redisTopicPartition);

    //setXgroup(redisHost, redisPort);

    // build redis source and stream
    RedisSourceConfig sourceConfig = RedisSourceConfig.builder().host(redisHost).port(redisPort)
        .consumerGroup("test-group").topicName(redisTopic).numPartitions(redisTopicPartition).startingId(StreamEntryID.XGROUP_LAST_ENTRY).build();
    RedisSource<RedisMessage> redisSource = new RedisSourceBuilder<>(sourceConfig,
        new RedisMessageDeserializer()).build();
    TypeInformation<RedisMessage> typeInfo = TypeInformation.of(RedisMessage.class);
    DataStream<RedisMessage> dataStream = env.fromSource(redisSource,
        WatermarkStrategy.noWatermarks(), "redis_data_stream", typeInfo);

    DataStream<Entry> entryStream = dataStream.map(EntrySerializer::of).keyBy(e -> e.getId());
    entryStream.print();
    env.execute(jobName);

  }

  private static void setXgroup(String redisHost, int redisPort) {
    //setup a consumer group for each partition
    try (JedisPooled jedis = new JedisPooled(redisHost, redisPort)) {

      //clean up for testing for now
      try {
        jedis.xgroupDestroy("test:0", "test-group");
        jedis.xgroupDestroy("test:1", "test-group");
        jedis.xgroupDestroy("test:2", "test-group");
        jedis.xgroupDestroy("test:3", "test-group");
        jedis.del("topic:test:config");
        jedis.ftDropIndexDD("test-split-index");
      } catch (Exception e) {
        //ignore
      }
      //StreamEntryID streamEntryID = new StreamEntryID(0, 0);
      StreamEntryID streamEntryID = StreamEntryID.XGROUP_LAST_ENTRY;
      jedis.xgroupCreate("test:0", "test-group", streamEntryID, true);
      jedis.xgroupCreate("test:1", "test-group", streamEntryID, true);
      jedis.xgroupCreate("test:2", "test-group", streamEntryID, true);
      jedis.xgroupCreate("test:3", "test-group", streamEntryID, true);
    }
  }
}
