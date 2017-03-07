/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.kql.util;

import io.confluent.kql.metastore.KQLTopic;
import io.confluent.kql.physical.GenericRow;

import jline.console.ConsoleReader;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KeyValueMapper;

import java.io.IOException;
import java.util.Map;


public class TopicPrinter {

  public void printGenericRowTopic(KQLTopic kqlTopic, ConsoleReader console, long interval, KQLConfig config) {

    KStreamBuilder builder = new KStreamBuilder();

    Map<String, Object> streamsProperties = config.originals();

    streamsProperties.put(
        StreamsConfig.APPLICATION_ID_CONFIG,
        kqlTopic.getKafkaTopicName() + "_" + System.currentTimeMillis()
    );

    streamsProperties.put(
        StreamsConfig.COMMIT_INTERVAL_MS_CONFIG,
        0
    );

    streamsProperties.put(
        StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG,
        0
    );

    KStream<String, GenericRow>
        source =
        builder.stream(Serdes.String(), SerDeUtil.getRowSerDe(kqlTopic.getKqlTopicSerDe()),
                       kqlTopic.getKafkaTopicName());

    source.map(new KQLPrintKeyValueMapper(console, interval));

    KafkaStreams streams = new KafkaStreams(builder, new StreamsConfig(streamsProperties));
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
          if (row != null) {
            console.println(row.toString());
          } else {
            console.println("null");
          }
        }

        recordIndex++;
      } catch (IOException e) {
        e.printStackTrace();
      }
      return new KeyValue<String, GenericRow>(key, row);
    }
  }

}
