package com.redis.end2end.scratch;

import com.redis.end2end.DataGenJob;
import com.redis.end2end.model.EnrichedTransaction;
import com.redis.end2end.model.Transaction;
import java.io.InputStream;
import java.util.Properties;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.json.JsonDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DataProcessingPipelineJob2 {

  public static void main(String[] args) throws Exception {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    //load configuration properties
    Properties config = new Properties();
    try (InputStream stream = DataGenJob.class.getClassLoader().getResourceAsStream(
        "end2end.properties")) {
      config.load(stream);
    }

    long checkpointInterval =  Long.parseLong(config.getProperty("flink.checkpointing.interval"));
    long bufferTimeout = Long.parseLong(config.getProperty("flink.buffer.timeout"));
    env.enableCheckpointing(checkpointInterval);
    env.setBufferTimeout(bufferTimeout);

    String kafkaTopic = config.getProperty("kafka.topic");
    String bootstrapServers = config.getProperty("kafka.bootstrap.servers");
    int redisTopicPartition = Integer.parseInt(config.getProperty("redis.topic.partitions"));

    env.setParallelism(redisTopicPartition);


    // build kafka source and stream
    KafkaSource<Transaction> transactionSource = KafkaSource.<Transaction>builder()
        .setBootstrapServers(bootstrapServers).setTopics(kafkaTopic)
        .setStartingOffsets(OffsetsInitializer.earliest())
        .setValueOnlyDeserializer(new JsonDeserializationSchema<>(Transaction.class)).build();

    DataStream<Transaction> transactionStream = env.fromSource(transactionSource,
        WatermarkStrategy.noWatermarks(), "transaction_kafka_topic");


    //make sure all processing for same account is done by same task to keep the order,transform from Transaction to EnrichedTransaction
    DataStream<Transaction> filteredStream = transactionStream.keyBy(t -> t.accountId).filter(t -> t.amount > 0).name("FilterPositiveTransactions");

    //transform and enrich
    DataStream<EnrichedTransaction>  enrichedTransactionStream =  filteredStream
        .map(t -> new EnrichedTransaction(t, 5)).name("EnrichedTransaction");

    enrichedTransactionStream.print();

    DataStream< Tuple3<String, Long,Long>> latencyStream = enrichedTransactionStream
        .map(t -> new Tuple3<String, Long ,Long>("Latency", t.accountId ,System.currentTimeMillis() - t.timestamp)).returns(Types.TUPLE(Types.STRING, Types.LONG,Types.LONG)
        ).name("CalculateLatency");;

    latencyStream.print();

    env.execute("BusinessPipelineJob");
  }
}
