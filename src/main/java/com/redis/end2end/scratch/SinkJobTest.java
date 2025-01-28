package com.redis.end2end.scratch;


import com.redis.end2end.scratch.model.Entry;
import com.redis.end2end.scratch.model.EntrySerializer;
import com.redis.flink.sink.RedisSerializer;
import com.redis.flink.sink.RedisSink;
import com.redis.flink.sink.RedisSinkBuilder;
import com.redis.flink.sink.RedisSinkConfig;
import java.io.InputStream;
import java.util.Random;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SinkJobTest {
static Logger LOG = LoggerFactory.getLogger(SinkJobTest.class);

  public static void main(String[] args) throws Exception {

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    ParameterTool argProperties = ParameterTool.fromArgs(args);
    ParameterTool config = null;

    try (InputStream stream = SinkJobTest.class.getClassLoader()
        .getResourceAsStream("end2end.properties")) {
      config = ParameterTool.fromPropertiesFile(stream);
    }

    //to display
    env.getConfig().setGlobalJobParameters(config.mergeWith(argProperties));

    String jobName = argProperties.get("jobname", "NO_NAME_SINK" + new Random().nextInt(1000));
    int rateLimit = argProperties.getInt("ratelimit", 10);
    long maxRecords = argProperties.getLong("maxrecords", Long.MAX_VALUE);
    int parallelism = argProperties.getInt("parallelism", 1);

    String redisHost = config.get("redis.host");
    int redisPort = config.getInt("redis.port");
    String redisTopic = config.get("redis.topic");
    int redisTopicPartition = config.getInt("redis.topic.partitions");
    int checkpointInterval = config.getInt("flink.checkpointing.interval");
    int flinkBufferTimeout = config.getInt("flink.buffer.timeout");


    env.enableCheckpointing(checkpointInterval);
    env.setBufferTimeout(flinkBufferTimeout);
    env.setParallelism(parallelism);

    //Sample data source
    DataGeneratorSource<Entry> dataSource = new DataGeneratorSource<>(
        SinkJobTest.GenerateData::getData, maxRecords, RateLimiterStrategy.perSecond(rateLimit),
        Types.POJO(Entry.class));
    DataStream<Entry> dataStream = env.fromSource(dataSource, WatermarkStrategy.noWatermarks(),
        "data_generator");

    //Redis Sink
    RedisSinkConfig sinkConfig = RedisSinkConfig.builder().host(redisHost).port(redisPort)
        .topicName(redisTopic).numPartitions(redisTopicPartition).build();

    RedisSink<Entry> redisSink = new RedisSinkBuilder<>(new EntrySerializer(), sinkConfig).build();
    dataStream.sinkTo(redisSink).name("data_redis_stream");


    env.execute(jobName);
  }
  
  public static class GenerateData {

    public static Entry getData(long id) {
      return new Entry(id,System.currentTimeMillis());
    }
  }

}



