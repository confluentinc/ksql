/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.ksql.datagen;

import io.confluent.ksql.physical.GenericRow;
import io.confluent.ksql.serde.json.KsqlJsonDeserializer;
import io.confluent.ksql.serde.json.KsqlJsonSerializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.kstream.Windowed;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;


public class JsonConsumer {

  static Serde<GenericRow> genericRowSerde = null;

  public void printGenericRowTopic(String topicName) {

    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG,
              "StreamExampleGenericRowProcessor-" + System.currentTimeMillis());
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

    // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 100);

    KStreamBuilder builder = new KStreamBuilder();

    KTable<String, GenericRow>
        source =
        builder.table(Serdes.String(), getGenericRowSerde(), topicName, "users");

    source.mapValues(new ValueMapper<GenericRow, GenericRow>() {
      @Override
      public GenericRow apply(GenericRow genericRow) {
        System.out.println(genericRow.toString());
        return genericRow;
      }
    });

    KafkaStreams streams = new KafkaStreams(builder, props);
    streams.start();

    // usually the stream application would be running forever,
    // in this example we just let it run for some time and stop since the input data is finite.
    try {
      Thread.sleep(5000L);
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

    KStreamBuilder builder = new KStreamBuilder();

    KStream<String, GenericRow>
        source =
        builder.stream(Serdes.String(), genericRowSerde, "StreamExample1-GenericRow-order");

    KStream<String, GenericRow> orderFilter = orderFilter(source);

    KStream<String, GenericRow> orderProject = orderProject(orderFilter);

    KTable<Windowed<String>, Long> orderAggregate = orderUnitsPer10Seconds(orderProject);

    orderAggregate.print();

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

  private KStream<String, GenericRow> orderFilter(KStream<String, GenericRow> orderStream) {

    KStream<String, GenericRow>
        orderFilter =
        orderStream.filter(new Predicate<String, GenericRow>() {
          @Override
          public boolean test(String key, GenericRow value) {
            int units = (Integer) value.getColumns().get(3);
            return units > 5;
          }
        });
    return orderFilter;
  }

  private KStream<String, GenericRow> orderProject(KStream<String, GenericRow> orderStream) {

    KStream<String, GenericRow>
        orderProject =
        orderStream.map(new KeyValueMapper<String, GenericRow, KeyValue<String, GenericRow>>() {
          @Override
          public KeyValue<String, GenericRow> apply(String key, GenericRow value) {
            List<Object>
                newColumns =
                Arrays.asList(value.getColumns().get(0), value.getColumns().get(1),
                              value.getColumns().get(3));
            GenericRow genericRow = new GenericRow(newColumns);
            return new KeyValue<String, GenericRow>(key, genericRow);
          }
        });
    return orderProject;
  }

  private KTable<Windowed<String>, Long> orderUnitsPer10Seconds(
      KStream<String, GenericRow> sourceOrderStream) {

    long windowSizeMs = 1000L;
    TimeWindows timeWindows = TimeWindows.of(10000);
    KTable<Windowed<String>, Long> groupedStream = sourceOrderStream
        .selectKey(new KeyValueMapper<String, GenericRow, String>() {
          @Override
          public String apply(String key, GenericRow value) {
            String newKey = value.getColumns().get(1).toString();
            return newKey;
          }
        })
        .groupByKey(Serdes.String(), getGenericRowSerde())
        .aggregate(
            new Initializer<Long>() {
              @Override
              public Long apply() {
                return 0L;
              }
            },
            new Aggregator<String, GenericRow, Long>() {
              @Override
              public Long apply(String aggKey, GenericRow value, Long aggregate) {
                long val = Long.valueOf(value.getColumns().get(2).toString());
                return aggregate + val;
              }
            },
            timeWindows,
            Serdes.Long(),
            "SalesUnits"
        );

    return groupedStream;

  }

  private static Serde<GenericRow> getGenericRowSerde() {
    if (genericRowSerde == null) {
      Map<String, Object> serdeProps = new HashMap<>();

      final Serializer<GenericRow> genericRowSerializer = new KsqlJsonSerializer(null);
      serdeProps.put("JsonPOJOClass", GenericRow.class);
      genericRowSerializer.configure(serdeProps, false);

      final Deserializer<GenericRow> genericRowDeserializer = new KsqlJsonDeserializer(null);
      serdeProps.put("JsonPOJOClass", GenericRow.class);
      genericRowDeserializer.configure(serdeProps, false);

      genericRowSerde = Serdes.serdeFrom(genericRowSerializer, genericRowDeserializer);
    }
    return genericRowSerde;
  }

  public void joinTest() {
    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG,
              "StreamExampleGenericRowProcessor-" + System.currentTimeMillis());
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

    // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 100);

    KStreamBuilder builder = new KStreamBuilder();

    KStream<String, GenericRow>
        pageviewStream =
        builder.stream(Serdes.String(), getGenericRowSerde(), "streams-pageview-input");

    KTable<String, GenericRow>
        usersTable =
        builder.table(Serdes.String(), getGenericRowSerde(), "streams-userprofile-input", "users");

    usersTable.mapValues(new ValueMapper<GenericRow, Object>() {
      @Override
      public GenericRow apply(GenericRow genericRow) {
        System.out.println(genericRow.toString());
        return genericRow;
      }
    });

    KafkaStreams streams = new KafkaStreams(builder, props);
    streams.start();

    // usually the stream application would be running forever,
    // in this example we just let it run for some time and stop since the input data is finite.
    try {
      Thread.sleep(5000L);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    streams.close();
  }

  public static void main(String[] args) {
//        new KSqlStreamProcessor().processGenericRow();
//        new KSqlStreamProcessor().printGenericRowTopic("streams-userprofile-input");
//        new KSqlStreamProcessor().printGenericRowTopic("FEMALEUSERS");
    new JsonConsumer().printGenericRowTopic("ENRICHEDFEMALE_CSV");
  }
}
