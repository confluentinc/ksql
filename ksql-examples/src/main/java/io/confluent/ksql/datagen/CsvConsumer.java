/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.ksql.datagen;

import io.confluent.ksql.physical.GenericRow;
import io.confluent.ksql.serde.csv.KsqlCsvDeserializer;
import io.confluent.ksql.serde.csv.KsqlCsvSerializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Created by hojjat on 1/3/17.
 */
public class CsvConsumer {

  static Serde<GenericRow> genericRowSerde = null;

  private static Serde<GenericRow> getGenericRowSerde() {
    if (genericRowSerde == null) {
      Map<String, Object> serdeProps = new HashMap<>();

      final Serializer<GenericRow> genericRowSerializer = new KsqlCsvSerializer();
      genericRowSerializer.configure(serdeProps, false);

      final Deserializer<GenericRow> genericRowDeserializer = new KsqlCsvDeserializer();
      genericRowDeserializer.configure(serdeProps, false);

      genericRowSerde = Serdes.serdeFrom(genericRowSerializer, genericRowDeserializer);
    }
    return genericRowSerde;
  }

  public void printGenericRowTopic(String topicName) {

    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG,
              "StreamExampleGenericRowProcessor-" + System.currentTimeMillis());
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

    // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 0);
    props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

    KStreamBuilder builder = new KStreamBuilder();

    KStream<String, GenericRow>
        source = builder.stream(Serdes.String(), getGenericRowSerde(), topicName);

    source.print();

    KafkaStreams streams = new KafkaStreams(builder, props);
    streams.start();

    // usually the stream application would be running forever,
    // in this example we just let it run for some time and stop since the input data is finite.
    try {
      Thread.sleep(10000L);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    streams.close();
  }

  public static void main(String[] args) {
//    new CsvConsumer().printGenericRowTopic("Order-csv-GenericRow");
    new CsvConsumer().printGenericRowTopic("ENRICHEDFEMALE_CSV");
  }
}
