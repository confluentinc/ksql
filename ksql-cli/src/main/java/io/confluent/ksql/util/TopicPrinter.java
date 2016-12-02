package io.confluent.ksql.util;


import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.physical.GenericRow;
import io.confluent.ksql.serde.JsonPOJODeserializer;
import io.confluent.ksql.serde.JsonPOJOSerializer;

import jline.console.ConsoleReader;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.*;

public class TopicPrinter {

  static Serde<GenericRow> genericRowSerde = null;

  public void printGenericRowTopic(String topicName, ConsoleReader console, long interval,
                                   Map<String, String> cliProperties) throws IOException {

    Properties ksqlProperties = new Properties();
    ksqlProperties
        .put(StreamsConfig.APPLICATION_ID_CONFIG, "KSQL-Default-" + System.currentTimeMillis());
    ksqlProperties
        .put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KSQLConfig.DEFAULT_BOOTSTRAP_SERVERS_CONFIG);
    ksqlProperties
        .put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, KSQLConfig.DEFAULT_AUTO_OFFSET_RESET_CONFIG);
    if (!cliProperties.get(KSQLConfig.PROP_FILE_PATH_CONFIG)
        .equalsIgnoreCase(KSQLConfig.DEFAULT_PROP_FILE_PATH_CONFIG)) {
      ksqlProperties.load(new FileReader(cliProperties.get(KSQLConfig.PROP_FILE_PATH_CONFIG)));
    }
    ksqlProperties
        .put(StreamsConfig.APPLICATION_ID_CONFIG, topicName + "_" + System.currentTimeMillis());
    ksqlProperties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                       ksqlProperties.get(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG));

    // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
    ksqlProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                       ksqlProperties.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG));

    KStreamBuilder builder = new KStreamBuilder();

    KStream<String, GenericRow>
        source =
        builder.stream(Serdes.String(), getGenericRowSerde(), topicName);

    source.map(new KSQLPrintKeyValueMapper(console, interval));

    KafkaStreams streams = new KafkaStreams(builder, ksqlProperties);
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

  private static Serde<GenericRow> getGenericRowSerde() {
    if (genericRowSerde == null) {
      Map<String, Object> serdeProps = new HashMap<>();

      final Serializer<GenericRow> genericRowSerializer = new JsonPOJOSerializer<>();
      serdeProps.put("JsonPOJOClass", GenericRow.class);
      genericRowSerializer.configure(serdeProps, false);

      final Deserializer<GenericRow> genericRowDeserializer = new JsonPOJODeserializer<>();
      serdeProps.put("JsonPOJOClass", GenericRow.class);
      genericRowDeserializer.configure(serdeProps, false);

      genericRowSerde = Serdes.serdeFrom(genericRowSerializer, genericRowDeserializer);
    }
    return genericRowSerde;
  }

  class KSQLPrintKeyValueMapper
      implements KeyValueMapper<String, GenericRow, KeyValue<String, GenericRow>> {

    long recordIndex = 0;
    long interval;
    ConsoleReader console;

    public KSQLPrintKeyValueMapper(ConsoleReader console, long interval) {
      this.console = console;
      this.interval = interval;
    }

    @Override
    public KeyValue<String, GenericRow> apply(String key, GenericRow row) {
      try {
        if (interval > 0) {
          if (recordIndex % interval == 0) {
            console.println(row.toString());
          }
        } else {
          console.println(row.toString());
        }

        recordIndex++;
      } catch (IOException e) {
        e.printStackTrace();
      }
      return new KeyValue<String, GenericRow>(key, row);
    }
  }

}
