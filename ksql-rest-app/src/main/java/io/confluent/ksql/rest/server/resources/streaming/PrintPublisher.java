/**
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.rest.server.resources.streaming;

import com.google.common.util.concurrent.ListeningScheduledExecutorService;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.connect.data.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.rest.server.resources.streaming.PollingSubscription.Pollable;
import io.confluent.ksql.rest.server.resources.streaming.TopicStream.Format;

import static io.confluent.ksql.rest.server.resources.streaming.TopicStream.Format.getFormatter;

public class PrintPublisher implements Flow.Publisher<Collection<String>> {

  private static final Logger log = LoggerFactory.getLogger(PrintPublisher.class);

  private final ListeningScheduledExecutorService exec;
  private final SchemaRegistryClient schemaRegistryClient;
  private final String topicName;
  private final boolean fromBeginning;
  private final Map<String, Object> consumerProperties;

  private boolean closed = false;
  private KafkaConsumer<String, Bytes> topicConsumer;

  public PrintPublisher(
      ListeningScheduledExecutorService exec,
      SchemaRegistryClient schemaRegistryClient,
      Map<String, Object> consumerProperties,
      String topicName,
      boolean fromBeginning
  ) {
    this.exec = exec;
    this.schemaRegistryClient = schemaRegistryClient;
    this.consumerProperties = consumerProperties;
    this.topicName = topicName;
    this.fromBeginning = fromBeginning;
  }

  @Override
  public void subscribe(Flow.Subscriber<Collection<String>> subscriber) {
    this.topicConsumer = new KafkaConsumer<>(
        consumerProperties,
        new StringDeserializer(),
        new BytesDeserializer()
    );

    log.info("Running consumer for topic {}", topicName);
    List<TopicPartition> topicPartitions = topicConsumer.partitionsFor(topicName)
        .stream()
        .map(partitionInfo -> new TopicPartition(partitionInfo.topic(), partitionInfo.partition()))
        .collect(Collectors.toList());
    topicConsumer.assign(topicPartitions);

    if (fromBeginning) {
      topicConsumer.seekToBeginning(topicPartitions);
    }
    subscriber.onSubscribe(new PollingSubscription<>(
        exec,
        subscriber,
        new Pollable<Collection<String>>() {
          Throwable error;
          Format format = Format.UNDEFINED;

          @Override
          public Schema getSchema() {
            return null;
          }

          @Override
          public Collection<String> poll() {
            try {
              ConsumerRecords<String, Bytes> records = topicConsumer.poll(0);
              if (records.isEmpty()) {
                return null;
              }
              return StreamSupport
                  .stream(records.records(topicName).spliterator(), false)
                  .map((record) -> {
                    if (record == null) {
                      return null;
                    }
                    if (format == TopicStream.Format.UNDEFINED) {
                      format = getFormatter(topicName, record, schemaRegistryClient);
                    }
                    try {
                      return format.print(record);
                    } catch (IOException e) {
                      log.warn("Exception formatting record", e);
                      return null;
                    }
                  })
                  .filter(Objects::nonNull)
                  .collect(Collectors.toList());
            } catch (Exception e) {
              error = e;
              return null;
            }
          }

          @Override
          public boolean hasError() {
            return error != null;
          }

          @Override
          public Throwable getError() {
            return error;
          }

          @Override
          public void close() {
            PrintPublisher.this.close();
          }
        }
    ));
  }

  public synchronized void close() {
    if (!closed) {
      log.info("Closing consumer for topic {}", topicName);
      closed = true;
      topicConsumer.close();
    }
  }
}
