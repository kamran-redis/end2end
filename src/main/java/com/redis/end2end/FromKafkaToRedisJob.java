package com.redis.end2end;

import com.redis.flink.sink.RedisObjectSerializer;
import com.redis.flink.sink.RedisSink;
import com.redis.flink.sink.RedisSinkBuilder;
import com.redis.flink.sink.RedisSinkConfig;
import com.redis.end2end.model.Transaction;
import java.io.InputStream;
import java.util.Properties;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.json.JsonDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FromKafkaToRedisJob {

  private static final Logger LOG = LoggerFactory.getLogger(FromKafkaToRedisJob.class);

  public static void main(String[] args) throws Exception {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    //load configuration properties
    Properties config = new Properties();
    try (InputStream stream = DataGenJob.class.getClassLoader().getResourceAsStream(
        "end2end.properties")) {
      config.load(stream);
    }

    //https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/overview/#controlling-latency
    long checkpointInterval =  Long.parseLong(config.getProperty("flink.checkpointing.interval"));
    long bufferTimeout = Long.parseLong(config.getProperty("flink.buffer.timeout"));
    env.enableCheckpointing(checkpointInterval);
    env.setBufferTimeout(bufferTimeout);


    String kafkaTopic = config.getProperty("kafka.topic");
    String bootstrapServers = config.getProperty("kafka.bootstrap.servers");
    String redisHost = config.getProperty("redis.host");
    int redisPort = Integer.parseInt(config.getProperty("redis.port"));
    String redisTopic = config.getProperty("redis.topic");
    int redisTopicPartition = Integer.parseInt(config.getProperty("redis.topic.partitions"));

    // build kafka source and stream
    KafkaSource<Transaction> transactionSource = KafkaSource.<Transaction>builder()
        .setBootstrapServers(bootstrapServers).setTopics(kafkaTopic)
        .setStartingOffsets(OffsetsInitializer.earliest())
        .setValueOnlyDeserializer(new JsonDeserializationSchema<>(Transaction.class)).build();

    DataStream<Transaction> transactionStream = env.fromSource(transactionSource,
        WatermarkStrategy.noWatermarks(), "transaction_kafka_topic");

    // build redis sink
    RedisSinkConfig sinkConfig = RedisSinkConfig.builder().host(redisHost).port(redisPort).topicName(redisTopic)
        .numPartitions(redisTopicPartition).build();

    RedisSink<Transaction> redisSink = new RedisSinkBuilder<>(
       new RedisObjectSerializer<Transaction>(), sinkConfig).keyExtractor(t-> Long.toString(t.getAccountId())).build();
    transactionStream.sinkTo(redisSink).name("transaction_redis_stream");

    env.execute("FromKafkaToRedisJob");
  }
  
  public static void defineWorkflow() {
    
  }
}
