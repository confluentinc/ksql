package io.confluent.kql.util;


import io.confluent.kql.metastore.KQLTopic;
import io.confluent.kql.physical.GenericRow;

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

    Properties kqlProperties = new Properties();
    kqlProperties
        .put(StreamsConfig.APPLICATION_ID_CONFIG, "KQL-Default-" + System.currentTimeMillis());
    kqlProperties
        .put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KQLConfig.DEFAULT_BOOTSTRAP_SERVERS_CONFIG);
    kqlProperties
        .put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, KQLConfig.DEFAULT_AUTO_OFFSET_RESET_CONFIG);
    if (!cliProperties.get(KQLConfig.PROP_FILE_PATH_CONFIG)
        .equalsIgnoreCase(KQLConfig.DEFAULT_PROP_FILE_PATH_CONFIG)) {
      kqlProperties.load(new FileReader(cliProperties.get(KQLConfig.PROP_FILE_PATH_CONFIG)));
    }
    kqlProperties
        .put(StreamsConfig.APPLICATION_ID_CONFIG, KQLTopic.getKafkaTopicName() + "_" + System.currentTimeMillis());
    kqlProperties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                       kqlProperties.get(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG));

    // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
    kqlProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                       kqlProperties.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG));


    KStreamBuilder builder = new KStreamBuilder();

    KStream<String, GenericRow>
        source =
        builder.stream(Serdes.String(), SerDeUtil.getRowSerDe(KQLTopic.getKqlTopicSerDe()),
                       KQLTopic.getKafkaTopicName());

    source.map(new KQLPrintKeyValueMapper(console, interval));

    KafkaStreams streams = new KafkaStreams(builder, kqlProperties);
    streams.start();

    // usually the stream application would be running forever,
    // in this example we just let it run for some time and stop since the input data is finite.
    try {
      Thread.sleep(3000L);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    streams.close();
  }

  class KQLPrintKeyValueMapper
      implements KeyValueMapper<String, GenericRow, KeyValue<String, GenericRow>> {

    long recordIndex = 0;
    long interval;
    ConsoleReader console;

    public KQLPrintKeyValueMapper(ConsoleReader console, long interval) {
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
