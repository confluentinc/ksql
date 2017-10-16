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
import io.confluent.ksql.serde.delimited.KsqlDelimitedDeserializer;
import io.confluent.ksql.serde.delimited.KsqlDelimitedSerializer;
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
public class DelimitedConsumer {

  static Serde<GenericRow> genericRowSerde = null;

  private static Serde<GenericRow> getGenericRowSerde() {
    if (genericRowSerde == null) {
      Map<String, Object> serdeProps = new HashMap<>();

      final Serializer<GenericRow> genericRowSerializer = new KsqlDelimitedSerializer();
      genericRowSerializer.configure(serdeProps, false);

      final Deserializer<GenericRow> genericRowDeserializer = new KsqlDelimitedDeserializer(null);
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
    streams.cleanUp();
  }

  public static void main(String[] args) {
    new DelimitedConsumer().printGenericRowTopic("ENRICHEDFEMALE_CSV");
  }
}
