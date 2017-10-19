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
import io.confluent.ksql.serde.avro.KsqlGenericRowAvroDeserializer;
import io.confluent.ksql.serde.avro.KsqlGenericRowAvroSerializer;
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
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Printed;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class AvroConsumer {

  static String schemaStr = "{"
                            + "\"namespace\": \"ksql\","
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
      serdeProps.put(KsqlGenericRowAvroSerializer.AVRO_SERDE_SCHEMA_CONFIG, schemaStr);

      final Serializer<GenericRow> genericRowSerializer = new KsqlGenericRowAvroSerializer(null);
      genericRowSerializer.configure(serdeProps, false);

      final Deserializer<GenericRow> genericRowDeserializer = new KsqlGenericRowAvroDeserializer(null);
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

    StreamsBuilder builder = new StreamsBuilder();

    KStream<String, GenericRow>
        source = builder.stream(topicName, Consumed.with(Serdes.String(), getGenericRowSerde()));

    source.print(Printed.toSysOut());

    KafkaStreams streams = new KafkaStreams(builder.build(), props);
    streams.start();

    // usually the stream application would be running forever,
    // in this example we just let it run for some time and stop since the input data is finite.
    try {
      Thread.sleep(10000L);
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
    props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 0);
    props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

    StreamsBuilder builder = new StreamsBuilder();

    KStream<String, GenericRow>
        source =
        builder.stream("StreamExample1-GenericRow-order", Consumed.with(Serdes.String(), genericRowSerde));

    source.map(new KSQLPrintKeyValueMapper());

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

  class KSQLPrintKeyValueMapper
      implements KeyValueMapper<String, GenericRow, KeyValue<String, GenericRow>> {

    public KSQLPrintKeyValueMapper() {
    }

    @Override
    public KeyValue<String, GenericRow> apply(String key, GenericRow row) {
      System.out.println(row);
      return new KeyValue<String, GenericRow>(key, row);
    }
  }

  public static void main(String[] args) {
    new AvroConsumer().printGenericRowTopic("bigorders_topic");
  }

}
