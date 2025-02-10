package com.redis.end2end.simple;

import com.redis.flink.sink.RedisObjectSerializer;
import com.redis.flink.sink.RedisSink;
import com.redis.flink.sink.RedisSinkBuilder;
import com.redis.flink.sink.RedisSinkConfig;
import com.redis.flink.source.partitioned.RedisSource;
import com.redis.flink.source.partitioned.RedisSourceBuilder;
import com.redis.flink.source.partitioned.RedisSourceConfig;
import com.redis.flink.source.partitioned.reader.deserializer.RedisObjectDeserializer;
import java.io.IOException;
import java.util.Random;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.apache.flink.api.common.SupportsConcurrentExecutionAttempts;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import redis.clients.jedis.StreamEntryID;

public class RedisSample {

  public static void main(String[] args) throws Exception {

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    //Configuration conf = new Configuration();
    //final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
    ParameterTool argProperties = ParameterTool.fromArgs(args);

    final String jobName = argProperties.get("jobname", "REDIS_SAMPLE_" + new Random().nextInt(1000));
    int rateLimit = argProperties.getInt("ratelimit", 1000);
    long maxRecords = argProperties.getLong("maxrecords", Long.MAX_VALUE);
    int parallelism = argProperties.getInt("parallelism", 1);
    String redisHost = argProperties.get("redis.host", "localhost");
    int redisPort = argProperties.getInt("redis.port", 6379);
    String redisPassword = argProperties.get("redis.password", "");
    String redisUser = argProperties.get("redis.user", "default");
    String redisTopic = argProperties.get("redis,topic", "simple:topic");
    String redisConsumerGroup = argProperties.get("redis.consumer.group", "simple:group");

    int redisTopicPartition = parallelism;

    int checkpointInterval = argProperties.getInt("flink.checkpointing.interval", 10);
    int flinkBufferTimeout = argProperties.getInt("flink.buffer.timeout", 1000);

    env.getConfig().setGlobalJobParameters(argProperties);
    env.enableCheckpointing(checkpointInterval);
    env.setBufferTimeout(flinkBufferTimeout);
    env.setParallelism(parallelism);

    //Sample data source
    DataGeneratorSource<Event> dataSource = new DataGeneratorSource<>(GenerateData::getData,
        maxRecords, RateLimiterStrategy.perSecond(rateLimit), Types.POJO(Event.class));
    DataStream<Event> dataStream = env.fromSource(dataSource, WatermarkStrategy.noWatermarks(),
        "data_generator");

    //Redis Sink


    RedisSinkConfig sinkConfig = RedisSinkConfig.builder().host(redisHost).port(redisPort).password(redisPassword).user(redisUser)
        .topicName(redisTopic).numPartitions(redisTopicPartition).build();

    RedisSink<Event> redisSink = new RedisSinkBuilder<>(new RedisObjectSerializer<Event>(),
        sinkConfig).keyExtractor(Event::getId).build();
    dataStream.sinkTo(redisSink).name("redis_event_sink");

    //Redis Source
    RedisSourceConfig sourceConfig = RedisSourceConfig.builder().host(redisHost).port(redisPort).password(redisPassword).user(redisUser)
        .consumerGroup(redisConsumerGroup).topicName(redisTopic).numPartitions(redisTopicPartition)
        .startingId(StreamEntryID.XGROUP_LAST_ENTRY).requireAck(true).build();

    RedisSource<Event> redisSource = new RedisSourceBuilder<>(sourceConfig,
        new RedisObjectDeserializer<>(Event.class)).build();

    TypeInformation<Event> typeInfo = TypeInformation.of(Event.class);
    DataStream<Event> eventStream = env.fromSource(redisSource, WatermarkStrategy.noWatermarks(),
        "redis_event_source", typeInfo);
    eventStream.map(new CalculateMetrics(1000)).name("calculate_metrics");
    eventStream.sinkTo(new NullSink<>()).name("null_sink");

    env.execute(jobName);
  }

  public static class GenerateData {

    public static Event getData(long id) {
      return new Event(Long.toString(id), System.currentTimeMillis());
    }
  }


  @AllArgsConstructor
  @NoArgsConstructor
  @ToString(includeFieldNames = true)
  @Data
  public static class Event {

    String id;
    long timestamp;
  }


  private static class NullSink<IN> implements Sink<IN>, SupportsConcurrentExecutionAttempts {

    @Override
    public SinkWriter<IN> createWriter(InitContext context) throws IOException {
      return getWriter();
    }

    @Override
    public SinkWriter<IN> createWriter(WriterInitContext context) throws IOException {

      return getWriter();
    }

    private SinkWriter<IN> getWriter() {
      return new SinkWriter<IN>() {
        @Override
        public void write(IN element, Context context) throws IOException, InterruptedException {
        }

        @Override
        public void flush(boolean endOfInput) throws IOException, InterruptedException {

        }

        @Override
        public void close() {
        }
      };
    }
  }

  private static class CalculateMetrics extends RichMapFunction<Event, Event> {

    // how many recent samples to use for computing the histogram statistics
    private final int entryTimeLagRingBufferSize;
    private transient DescriptiveStatisticsHistogram eventTimeLag;
    private transient Counter counter;


    public CalculateMetrics(int entryTimeLagRingBufferSize) {
      this.entryTimeLagRingBufferSize = entryTimeLagRingBufferSize;

    }

    @Override
    public Event map(Event event) {
      eventTimeLag.update(System.currentTimeMillis() - event.timestamp);
      counter.inc();
      return event;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
      super.open(parameters);
      eventTimeLag = getRuntimeContext().getMetricGroup().histogram(getRuntimeContext().getJobInfo().getJobName() + "_" + "MY1eventTimeLag",
          new DescriptiveStatisticsHistogram(entryTimeLagRingBufferSize));
      counter = getRuntimeContext()
          .getMetricGroup()
          .counter(getRuntimeContext().getJobInfo().getJobName() + "_"  + "MY2eventCounter");
    }
  }
}
