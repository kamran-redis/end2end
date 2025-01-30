package com.redis.end2end;

import com.mongodb.client.model.InsertOneModel;
import com.redis.end2end.model.EnrichedTransaction;
import com.redis.end2end.model.Transaction;
import com.redis.end2end.metrics.PipelineMetrics;
import com.redis.flink.sink.RedisObjectSerializer;
import com.redis.flink.sink.RedisSink;
import com.redis.flink.sink.RedisSinkBuilder;
import com.redis.flink.sink.RedisSinkConfig;
import com.redis.flink.source.partitioned.RedisSource;
import com.redis.flink.source.partitioned.RedisSourceBuilder;
import com.redis.flink.source.partitioned.RedisSourceConfig;
import com.redis.flink.source.partitioned.reader.deserializer.RedisObjectDeserializer;
import java.io.InputStream;
import java.util.Properties;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.mongodb.sink.MongoSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.bson.BsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.StreamEntryID;

public class DataProcessingPipelineJob {

  private static final Logger LOG = LoggerFactory.getLogger(DataProcessingPipelineJob.class);

  public static void main(String[] args) throws Exception {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    //load configuration properties
    Properties config = new Properties();
    try (InputStream stream = DataGenJob.class.getClassLoader()
        .getResourceAsStream("end2end.properties")) {
      config.load(stream);
    }

    int redisTopicPartition = Integer.parseInt(config.getProperty("redis.topic.partitions"));
    long checkpointInterval = Long.parseLong(config.getProperty("flink.checkpointing.interval"));
    long bufferTimeout = Long.parseLong(config.getProperty("flink.buffer.timeout"));
    String redisHost = config.getProperty("redis.host");
    int redisPort = Integer.parseInt(config.getProperty("redis.port"));
    String redisTopic = config.getProperty("redis.topic");

    //setup flink environment parallelism and checkpointing
    env.enableCheckpointing(checkpointInterval);
    env.setBufferTimeout(bufferTimeout);
    env.setParallelism(redisTopicPartition);

    // build redis source and stream
    RedisSourceConfig sourceConfig = RedisSourceConfig.builder().host(redisHost).port(redisPort)
        .consumerGroup("transaction-group").topicName(redisTopic).numPartitions(redisTopicPartition)
        .startingId(StreamEntryID.XGROUP_LAST_ENTRY).requireAck(true).build();

    RedisSource<Transaction> redisSource = new RedisSourceBuilder<>(sourceConfig,
        new RedisObjectDeserializer(Transaction.class)).build();

    TypeInformation<Transaction> typeInfo = TypeInformation.of(Transaction.class);
    DataStream<Transaction> transactionStream = env.fromSource(redisSource,
        WatermarkStrategy.noWatermarks(), "redis_transaction_stream", typeInfo);


    /* make sure all processing for same account is done by same taskmanager to keep the order(keyby accountId)
     Transctions with zero or less value are sent to DeadLetterStream using OutputTag */
    final OutputTag<Transaction> DLSTag = new OutputTag<Transaction>("DLSOutput") {
    };

    SingleOutputStreamOperator<Transaction> filteredStream = transactionStream.keyBy(
            t -> t.accountId).process(new FilterTransactionFunction(DLSTag))
        .name("FilterPositiveTransactions");

    //transform and enrich
    DataStream<EnrichedTransaction> enrichedTransactionStream = filteredStream.map(
        t -> new EnrichedTransaction(t, 5)).name("EnrichedTransaction");

    enrichedTransactionStream.print();
    MongoSink<EnrichedTransaction> mongoSink = MongoSink.<EnrichedTransaction>builder()
        .setUri("mongodb://mongo:passwordm@mongo:27017/").setDatabase("transaction_db")
        .setCollection("transactions").setBatchSize(1000).setBatchIntervalMs(1000).setMaxRetries(3)
        .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE).setSerializationSchema(
            (transaction, context) -> new InsertOneModel<>(BsonDocument.parse(
                "{_id: " + transaction.accountId + ", amount: " + transaction.amount
                    + ", timestamp: " + transaction.timestamp + ", transactionFee: "
                    + transaction.transactionFee + "}"))).build();
    enrichedTransactionStream.sinkTo(mongoSink);

    //Dead letter stream
    DataStream<Transaction> DLSOutput = filteredStream.getSideOutput(DLSTag);
    // build redis sink for DLS
    String deadLetterTopic = config.getProperty("redis.dls.topic");
    RedisSinkConfig sinkConfig = RedisSinkConfig.builder().host(redisHost).port(redisPort)
        .topicName(deadLetterTopic).numPartitions(redisTopicPartition).build();

    RedisSink<Transaction> redisDLSSink = new RedisSinkBuilder<>(
        new RedisObjectSerializer<Transaction>(), sinkConfig).keyExtractor(
        t -> Long.toString(t.getAccountId())).build();
    DLSOutput.sinkTo(redisDLSSink).name("transaction_DLS_redis_stream");

    env.execute("BusinessPipelineJob");
  }

  public static void buildWorkflow() {

  }


  private static class FilterTransactionFunction extends ProcessFunction<Transaction, Transaction> {
    private final OutputTag<Transaction> DLSTag;
    private transient PipelineMetrics metrics;

    public FilterTransactionFunction(OutputTag<Transaction> DLSTag) {
      this.DLSTag = DLSTag;
    }

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
      super.open(parameters);
      metrics = new PipelineMetrics(getRuntimeContext());
    }

    @Override
    public void processElement(Transaction transaction, Context ctx, Collector<Transaction> out)
        throws Exception {
      long startTime = System.currentTimeMillis();
      metrics.incrementTotalTransactions();
      
      try {
        if (transaction.amount <= 0) {
          metrics.incrementInvalidTransactions();
          ctx.output(DLSTag, transaction);
          return;
        }
        metrics.incrementValidTransactions();
        out.collect(transaction);
      } catch (Exception e) {
        metrics.incrementProcessingErrors();
        throw e;
      } finally {
        metrics.recordProcessingTime(System.currentTimeMillis() - startTime);
      }
    }
  }
}
