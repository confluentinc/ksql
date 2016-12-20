package io.confluent.ksql.util;


import io.confluent.ksql.metastore.KQLTopic;
import io.confluent.ksql.physical.GenericRow;

import jline.console.ConsoleReader;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class TopicPrinter {

  public void printGenericRowTopic(KQLTopic KQLTopic, ConsoleReader console, long interval,
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
        .put(StreamsConfig.APPLICATION_ID_CONFIG, KQLTopic.getKafkaTopicName() + "_" + System.currentTimeMillis());
    ksqlProperties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                       ksqlProperties.get(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG));

    // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
    ksqlProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                       ksqlProperties.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG));


    KStreamBuilder builder = new KStreamBuilder();

    KStream<String, GenericRow>
        source =
        builder.stream(Serdes.String(), SerDeUtil.getRowSerDe(KQLTopic.getKqlTopicSerDe()),
                       KQLTopic.getKafkaTopicName());

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
