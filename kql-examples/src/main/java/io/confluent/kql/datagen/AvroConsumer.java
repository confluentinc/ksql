/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.kql.datagen;

import io.confluent.kql.physical.GenericRow;
import io.confluent.kql.serde.avro.KQLGenericRowAvroDeserializer;
import io.confluent.kql.serde.avro.KQLGenericRowAvroSerializer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KeyValueMapper;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class AvroConsumer {

  static String schemaStr = "{"
                            + "\"namespace\": \"kql\","
                            + " \"name\": \"orders\","
                            + " \"type\": \"record\","
                            + " \"fields\": ["
                            + "     {\"name\": \"ordertime\", \"type\": \"long\"},"
                            + "     {\"name\": \"orderid\",  \"type\": \"string\"},"
                            + "     {\"name\": \"itemid\", \"type\": \"string\"},"
                            + "     {\"name\": \"orderunits\", \"type\": \"double\"}"
                            + " ]"
                            + "}";

  static Serde<GenericRow> genericRowSerde = null;

  private static Serde<GenericRow> getGenericRowSerde() {
    if (genericRowSerde == null) {
      Map<String, Object> serdeProps = new HashMap<>();
      serdeProps.put(KQLGenericRowAvroSerializer.AVRO_SERDE_SCHEMA_CONFIG, schemaStr);

      final Serializer<GenericRow> genericRowSerializer = new KQLGenericRowAvroSerializer(null);
      genericRowSerializer.configure(serdeProps, false);

      final Deserializer<GenericRow> genericRowDeserializer = new KQLGenericRowAvroDeserializer(null);
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
    props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 0);
    props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

    // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

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

  public void processGenericRow() {

    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "StreamExample1-GenericRow-Processor");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

    // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 0);
    props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

    KStreamBuilder builder = new KStreamBuilder();

    KStream<String, GenericRow>
        source =
        builder.stream(Serdes.String(), genericRowSerde, "StreamExample1-GenericRow-order");

    source.map(new KQLPrintKeyValueMapper());

    KafkaStreams streams = new KafkaStreams(builder, props);
    streams.start();

    // usually the stream application would be running forever,
    // in this example we just let it run for some time and stop since the input data is finite.
    try {
      Thread.sleep(1000L);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    streams.close();
  }

  class KQLPrintKeyValueMapper
      implements KeyValueMapper<String, GenericRow, KeyValue<String, GenericRow>> {

    public KQLPrintKeyValueMapper() {
    }

    @Override
    public KeyValue<String, GenericRow> apply(String key, GenericRow row) {
      System.out.println(row);
      return new KeyValue<String, GenericRow>(key, row);
    }
  }

  public static void main(String[] args) {
//    new AvroConsumer().printGenericRowTopic("StreamExample1-GenericRow-order");
    new AvroConsumer().printGenericRowTopic("bigorders_topic");
  }

}
