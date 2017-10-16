/**
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.datagen;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.serde.json.KsqlJsonDeserializer;
import io.confluent.ksql.serde.json.KsqlJsonSerializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Serialized;
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

    StreamsBuilder builder = new StreamsBuilder();

    KTable<String, GenericRow>
        source =
        builder.table(topicName, Consumed.with(Serdes.String(), getGenericRowSerde()));

    source.mapValues(genericRow -> {
      System.out.println(genericRow.toString());
      return genericRow;
    });

    KafkaStreams streams = new KafkaStreams(builder.build(), props);
    streams.start();

    // usually the stream application would be running forever,
    // in this example we just let it run for some time and stop since the input data is finite.
    try {
      Thread.sleep(5000L);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    streams.close();
    streams.cleanUp();
  }

  public void processGenericRow() {

    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "StreamExample1-GenericRow-Processor");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

    // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    StreamsBuilder builder = new StreamsBuilder();

    KStream<String, GenericRow>
        source =
        builder.stream("StreamExample1-GenericRow-order", Consumed.with(Serdes.String(), genericRowSerde));

    KStream<String, GenericRow> orderFilter = orderFilter(source);

    KStream<String, GenericRow> orderProject = orderProject(orderFilter);

    KTable<Windowed<String>, Long> orderAggregate = orderUnitsPer10Seconds(orderProject);

    orderAggregate.toStream().print(Printed.toSysOut());

    KafkaStreams streams = new KafkaStreams(builder.build(), props);
    streams.start();

    // usually the stream application would be running forever,
    // in this example we just let it run for some time and stop since the input data is finite.
    try {
      Thread.sleep(1000L);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    streams.close();
    streams.cleanUp();
  }

  private KStream<String, GenericRow> orderFilter(KStream<String, GenericRow> orderStream) {

    KStream<String, GenericRow>
        orderFilter =
        orderStream.filter((key, value) -> {
          int units = (Integer) value.getColumns().get(3);
          return units > 5;
        });
    return orderFilter;
  }

  private KStream<String, GenericRow> orderProject(KStream<String, GenericRow> orderStream) {

    KStream<String, GenericRow>
        orderProject =
        orderStream.map((KeyValueMapper<String, GenericRow, KeyValue<String, GenericRow>>) (key, value) -> {
          List<Object>
              newColumns =
              Arrays.asList(value.getColumns().get(0), value.getColumns().get(1),
                            value.getColumns().get(3));
          GenericRow genericRow = new GenericRow(newColumns);
          return new KeyValue<>(key, genericRow);
        });
    return orderProject;
  }

  private KTable<Windowed<String>, Long> orderUnitsPer10Seconds(
      KStream<String, GenericRow> sourceOrderStream) {

    TimeWindows timeWindows = TimeWindows.of(10000);
    KTable<Windowed<String>, Long> groupedStream = sourceOrderStream
        .selectKey((key, value) -> {
          String newKey = value.getColumns().get(1).toString();
          return newKey;
        })
        .groupByKey(Serialized.with(Serdes.String(), getGenericRowSerde()))
        .windowedBy(timeWindows)
        .aggregate(
            () -> 0L,
            (aggKey, value, aggregate) -> {
              long val = Long.valueOf(value.getColumns().get(2).toString());
              return aggregate + val;
            },
            Materialized.with(null, Serdes.Long())
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

    StreamsBuilder builder = new StreamsBuilder();

    KStream<String, GenericRow>
        pageviewStream =
        builder.stream("streams-pageview-input", Consumed.with(Serdes.String(), getGenericRowSerde()));

    KTable<String, GenericRow>
        usersTable =
        builder.table("streams-userprofile-input", Consumed.with(Serdes.String(), getGenericRowSerde()));

    usersTable.mapValues((ValueMapper<GenericRow, Object>) genericRow -> {
      System.out.println(genericRow.toString());
      return genericRow;
    });

    KafkaStreams streams = new KafkaStreams(builder.build(), props);
    streams.start();

    // usually the stream application would be running forever,
    // in this example we just let it run for some time and stop since the input data is finite.
    try {
      Thread.sleep(5000L);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    streams.close();
    streams.cleanUp();
  }

  public static void main(String[] args) {
    new JsonConsumer().printGenericRowTopic("ENRICHEDFEMALE_CSV");
  }
}
