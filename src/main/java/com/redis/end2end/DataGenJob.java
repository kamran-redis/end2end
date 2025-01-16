package com.redis.end2end;

import static java.lang.Long.parseLong;

import com.redis.end2end.model.Transaction;
import java.io.InputStream;
import java.util.Properties;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.formats.json.JsonSerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DataGenJob {

  public static void main(String[] args) throws Exception {

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);


    //load kafka connection properties
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

    //Generate Sample data
    DataGeneratorSource<Transaction> transactionSource = new DataGeneratorSource<>(
        GenerateTransaction::getTransaction, Long.MAX_VALUE, RateLimiterStrategy.perSecond(10),
        Types.POJO(Transaction.class));

    //Create a stream from the source
    DataStream<Transaction> transactionStream = env.fromSource(transactionSource,
        WatermarkStrategy.noWatermarks(), "transaction_generator");

    //Create a Kafka Sink
    KafkaRecordSerializationSchema<Transaction> transactionSerializer = KafkaRecordSerializationSchema.<Transaction>builder()
        .setTopic(kafkaTopic)
        .setValueSerializationSchema(new JsonSerializationSchema<>())
        .setKeySerializationSchema(transaction -> String.valueOf(transaction.getAccountId()).getBytes())
        .build();

    KafkaSink<Transaction> transactionKafkaSink = KafkaSink.<Transaction>builder()
        .setBootstrapServers(bootstrapServers).setRecordSerializer(transactionSerializer)
        .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE).build();

    transactionStream.sinkTo(transactionKafkaSink).name("transaction_kafka_topic");

    //transactionStream.print();
    env.execute("DataGenJob");
  }

  public static class GenerateTransaction {

    public static Transaction getTransaction(long id) {

      return new Transaction(id , System.currentTimeMillis(), id % 10);
    }

  }

}



