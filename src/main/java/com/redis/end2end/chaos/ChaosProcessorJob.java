package com.redis.end2end.chaos;

import com.redis.flink.source.partitioned.RedisSource;
import com.redis.flink.source.partitioned.RedisSourceBuilder;
import com.redis.flink.source.partitioned.RedisSourceConfig;
import com.redis.flink.source.partitioned.reader.deserializer.RedisObjectDeserializer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import redis.clients.jedis.StreamEntryID;

public class ChaosProcessorJob {
  static String redisHost;
  static int redisPort ;
  static String redisTopic;
  static String redisConsumerGroup;
  static int redisTopicPartition;
  static int parallelism;
  static int checkpointInterval;
  static int flinkBufferTimeout;

  public static void main(String[] args) throws Exception {

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    ParameterTool argProperties = ParameterTool.fromArgs(args);
    redisHost = argProperties.get("redis.host", "localhost");
    redisPort = argProperties.getInt("redis.port", 6379);
    redisTopic = argProperties.get("redis.topic", "chaos:topic");
    redisConsumerGroup = argProperties.get("redis.consumer.group", "chaos:group");


    parallelism = argProperties.getInt("parallelism", 1);
    redisTopicPartition = parallelism;
    checkpointInterval = argProperties.getInt("flink.checkpointing.interval", 10000);
    flinkBufferTimeout = argProperties.getInt("flink.buffer.timeout", 1000);


    env.setParallelism(parallelism);
    env.enableCheckpointing(checkpointInterval);

    buildWorkflow(env);
    env.execute("ChaosProcessorJob");
  }

  public static void buildWorkflow(StreamExecutionEnvironment env) {
    RedisSourceConfig sourceConfig = RedisSourceConfig.builder().host(redisHost).port(redisPort)
        .consumerGroup(redisConsumerGroup).topicName(redisTopic).numPartitions(redisTopicPartition)
        .startingId(StreamEntryID.XGROUP_LAST_ENTRY).requireAck(true).build();

    RedisSource<ChaosEvent> redisSource = new RedisSourceBuilder<>(sourceConfig,
        new RedisObjectDeserializer(ChaosEvent.class)).build();

    TypeInformation<ChaosEvent> typeInfo = TypeInformation.of(ChaosEvent.class);
    DataStream<ChaosEvent> eventStream = env.fromSource(redisSource,
        WatermarkStrategy.noWatermarks(), "redis_processing_stream", typeInfo);

    eventStream.print();
  }
}
