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
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class SourceJobTest {

  public static void main(String[] args) throws Exception {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    ParameterTool argProperties = ParameterTool.fromArgs(args);
    ParameterTool config = null;

    try (InputStream stream = SourceJobTest.class.getClassLoader()
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

    // build redis source and stream
    RedisSourceConfig sourceConfig = RedisSourceConfig.builder().host(redisHost).port(redisPort)
        .consumerGroup("test-group").topicName(redisTopic).numPartitions(redisTopicPartition)
        .build();
    RedisSource<RedisMessage> redisSource = new RedisSourceBuilder<>(sourceConfig,
        new RedisMessageDeserializer()).build();
    TypeInformation<RedisMessage> typeInfo = TypeInformation.of(RedisMessage.class);
    DataStream<RedisMessage> dataStream = env.fromSource(redisSource,
        WatermarkStrategy.noWatermarks(), "redis_data_stream", typeInfo);

    DataStream<Entry> entryStream = dataStream.map(EntrySerializer::of).keyBy(e -> e.getId())
        .map(new MeasureLatency(1000)).name("EntryMeasureLatency");
    //entryStream.print();


    env.execute(jobName);

  }



  private static class MeasureLatency extends RichMapFunction<Entry, Entry> {

    // how many recent samples to use for computing the histogram statistics
    private final int entryTimeLagRingBufferSize;
    private transient DescriptiveStatisticsHistogram entryTimeLag;

    public MeasureLatency(int entryTimeLagRingBufferSize) {
      this.entryTimeLagRingBufferSize = entryTimeLagRingBufferSize;
    }

    @Override
    public Entry map(Entry entry) {
      entryTimeLag.update(System.currentTimeMillis() - entry.timestamp);
      return entry;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
      super.open(parameters);
      entryTimeLag = getRuntimeContext().getMetricGroup().histogram("entryTimeLag",
          new DescriptiveStatisticsHistogram(entryTimeLagRingBufferSize));
    }
  }

}
