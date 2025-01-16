package com.redis.end2end;

import com.redis.end2end.model.EnrichedTransaction;
import com.redis.end2end.model.Transaction;
import com.redis.end2end.model.TransactionMapper;
import com.redis.flink.RedisMessage;
import com.redis.flink.source.partitioned.RedisSource;
import com.redis.flink.source.partitioned.RedisSourceBuilder;
import com.redis.flink.source.partitioned.RedisSourceConfig;
import com.redis.flink.source.partitioned.reader.deserializer.RedisMessageDeserializer;
import java.io.InputStream;
import java.util.Properties;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.StreamEntryID;

public class DataProcessingPipelineJob {

  public static void main(String[] args) throws Exception {

    try (JedisPooled jedis = new JedisPooled("redis", 6379)) {

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
      StreamEntryID streamEntryID = new StreamEntryID(0, 0);
      //StreamEntryID streamEntryID =StreamEntryID.XGROUP_LAST_ENTRY;
      jedis.xgroupCreate("test:0", "test-group", streamEntryID, true);
      jedis.xgroupCreate("test:1", "test-group", streamEntryID, true);
      jedis.xgroupCreate("test:2", "test-group", streamEntryID, true);
      jedis.xgroupCreate("test:3", "test-group", streamEntryID, true);
    }

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    //load configuration properties
    Properties config = new Properties();
    try (InputStream stream = DataGenJob.class.getClassLoader()
        .getResourceAsStream("end2end.properties")) {
      config.load(stream);
    }

    int redisTopicPartition = Integer.parseInt(config.getProperty("redis.topic.partitions"));
    long checkpointInterval =  Long.parseLong(config.getProperty("flink.checkpointing.interval"));
    long bufferTimeout = Long.parseLong(config.getProperty("flink.buffer.timeout"));
    env.enableCheckpointing(checkpointInterval);
    env.setBufferTimeout(bufferTimeout);
    env.setParallelism(redisTopicPartition);

    String redisHost = config.getProperty("redis.host");
    int redisPort = Integer.parseInt(config.getProperty("redis.port"));
    String redisTopic = config.getProperty("redis.topic");

    // build redis source and stream
    RedisSourceConfig sourceConfig = RedisSourceConfig.builder().host(redisHost).port(redisPort)
        .consumerGroup("test-group").topicName(redisTopic).numPartitions(redisTopicPartition)
        .build();
    RedisSource<RedisMessage> redisSource = new RedisSourceBuilder<>(sourceConfig,
        new RedisMessageDeserializer()).build();
    TypeInformation<RedisMessage> typeInfo = TypeInformation.of(RedisMessage.class);
    DataStream<RedisMessage> redisStream = env.fromSource(redisSource,
        WatermarkStrategy.noWatermarks(), "redis_transaction_stream", typeInfo);

    //transform from RedisMessage to Transaction
    DataStream<Transaction> transactionStream = redisStream.map(TransactionMapper::of).name("TransformRedisMessageToTransaction");

    //make sure all processing for same account is done by same task to keep the order(keyby accountId,transform from Transaction to EnrichedTransaction
    DataStream<Transaction> filteredStream = transactionStream.keyBy(t -> t.accountId).filter(t -> t.amount > 0).name("FilterPositiveTransactions");

    //transform and enrich
    DataStream<EnrichedTransaction>  enrichedTransactionStream =  filteredStream
        .map(t -> new EnrichedTransaction(t, 5)).name("EnrichedTransaction");

    enrichedTransactionStream.print();
    /*DataStream< Tuple3<String, Long,Long>> latencyStream = enrichedTransactionStream
        .map(t -> new Tuple3<String, Long ,Long>("Latency", t.accountId ,System.currentTimeMillis() - t.timestamp)).returns(Types.TUPLE(Types.STRING, Types.LONG,Types.LONG)
        ).name("CalculateLatency");;

    latencyStream.print();*/

    env.execute("BusinessPipelineJob");
  }
}
