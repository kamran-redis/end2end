package com.redis.end2end.chaos;

import com.redis.flink.sink.RedisObjectSerializer;
import com.redis.flink.sink.RedisSink;
import com.redis.flink.sink.RedisSinkBuilder;
import com.redis.flink.sink.RedisSinkConfig;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ChaosProducerJob {

  private static final Logger LOG = LoggerFactory.getLogger(ChaosProducerJob.class);

  static String redisHost;
  static int redisPort ;
  static String redisTopic;
  static int parallelism;
  static int checkpointInterval;
  static int flinkBufferTimeout;

  public static void main(String args[]) throws Exception {


    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    ParameterTool argProperties = ParameterTool.fromArgs(args);
    redisHost = argProperties.get("redis.host", "localhost");
    redisPort = argProperties.getInt("redis.port", 6379);
    redisTopic = argProperties.get("redis.topic", "chaos:topic");

    parallelism = argProperties.getInt("parallelism", 1);
    checkpointInterval = argProperties.getInt("flink.checkpointing.interval", 10000);
    flinkBufferTimeout = argProperties.getInt("flink.buffer.timeout", 1000);


    env.setParallelism(parallelism);
    env.enableCheckpointing(checkpointInterval);

    buildWorkflow(env);
    env.execute("ChaosProducerJob");

    //generate predictable data
    //Generate Sample data

    //write to Redis stream
    //check output matches redis streams

    //for serialization testing write to

  }

  public static void buildWorkflow(StreamExecutionEnvironment env) {
    DataGeneratorSource<ChaosEvent> source = new DataGeneratorSource<>(
        GenerateData::getTransaction, Long.MAX_VALUE, RateLimiterStrategy.perSecond(1),
        Types.POJO(ChaosEvent.class));
    //Create a stream from the source
    DataStream<ChaosEvent> transactionStream = env.fromSource(source,
        WatermarkStrategy.noWatermarks(), "chaos_generator");

    RedisSinkConfig sinkConfig = RedisSinkConfig.builder().host(redisHost).port(redisPort)
        .topicName(redisTopic)
        .numPartitions(1).build();


    RedisSink<ChaosEvent> redisSink = new RedisSinkBuilder<>(
        new RedisObjectSerializer<ChaosEvent>(), sinkConfig).keyExtractor(t-> Long.toString(t.getId())).build();
    transactionStream.sinkTo(redisSink).name("redis_stream");
  }

  public static class GenerateData {

    public static ChaosEvent getTransaction(long id) {
      System.out.println(id);
      return new  ChaosEvent(id, System.currentTimeMillis(), Long.toString(id));
    }
  }
}