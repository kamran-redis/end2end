package com.redis.end2end.scratch;



import com.mongodb.client.model.InsertOneModel;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.mongodb.sink.MongoSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.bson.BsonDocument;


public class DataGenJob2 {


  static class Book {
    public Book(Long id, String title, String authors, Integer year) {
      this.id = id;
      this.title = title;
      this.authors = authors;
      this.year = year;
    }
    final Long id;
    final String title;
    final String authors;
    final Integer year;

    @Override
    public String toString() {
      return "Book{" +
          "id=" + id +
          ", title='" + title + '\'' +
          ", authors='" + authors + '\'' +
          ", year=" + year +
          '}';
    }
  }

  public static void main(String[] args) throws Exception {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();

    DataStream<Book> bookStream = env.fromElements(
        new Book(101L, "Stream Processing with Apache Flink", "Fabian Hueske, Vasiliki Kalavri", 2019),
        new Book(102L, "Streaming Systems", "Tyler Akidau, Slava Chernyak, Reuven Lax", 2018),
        new Book(103L, "Designing Data-Intensive Applications", "Martin Kleppmann", 2017),
        new Book(104L, "Kafka: The Definitive Guide", "Gwen Shapira, Neha Narkhede, Todd Palino", 2017)
    );
    bookStream.print();

    MongoSink<Book> sink = MongoSink.<Book>builder()
        .setUri("mongodb://root:example@mongo:27017/")
        .setDatabase("my_db")
        .setCollection("my_coll")
        .setBatchSize(1000)
        .setBatchIntervalMs(1000)
        .setMaxRetries(3)
        .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .setSerializationSchema(
            (input, context) -> new InsertOneModel<>(BsonDocument.parse(
                "{_id: " + input.id + ", title: '" + input.title + "', authors: '" + input.authors + "', year: " + input.year + "}")))
        .build();
    bookStream.sinkTo(sink);


    env.execute();
  }
}



