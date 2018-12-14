/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.rest.server.resources.streaming;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.rest.server.resources.streaming.TopicStream.RecordFormatter;
import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.ws.rs.core.StreamingOutput;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TopicStreamWriter implements StreamingOutput {

  private static final Logger log = LoggerFactory.getLogger(TopicStreamWriter.class);
  private final Long interval;
  private final Duration disconnectCheckInterval;
  private final KafkaConsumer<String, Bytes> topicConsumer;
  private final SchemaRegistryClient schemaRegistryClient;
  private final String topicName;

  private long messagesWritten;

  public TopicStreamWriter(
      final SchemaRegistryClient schemaRegistryClient,
      final Map<String, Object> consumerProperties,
      final String topicName,
      final long interval,
      final Duration disconnectCheckInterval,
      final boolean fromBeginning
  ) {
    this.schemaRegistryClient = schemaRegistryClient;
    this.topicName = topicName;
    this.messagesWritten = 0;

    this.disconnectCheckInterval = Objects
        .requireNonNull(disconnectCheckInterval, "disconnectCheckInterval");

    this.topicConsumer = new KafkaConsumer<>(
        consumerProperties,
        new StringDeserializer(),
        new BytesDeserializer()
    );

    final List<TopicPartition> topicPartitions = topicConsumer.partitionsFor(topicName)
        .stream()
        .map(partitionInfo -> new TopicPartition(partitionInfo.topic(), partitionInfo.partition()))
        .collect(Collectors.toList());
    topicConsumer.assign(topicPartitions);

    if (fromBeginning) {
      topicConsumer.seekToBeginning(topicPartitions);
    }

    this.interval = interval;
  }

  @SuppressWarnings("InfiniteLoopStatement")
  @Override
  public void write(final OutputStream out) {
    try {
      final RecordFormatter formatter = new RecordFormatter(schemaRegistryClient, topicName);
      boolean printFormat = true;
      while (true) {
        final ConsumerRecords<String, Bytes> records = topicConsumer.poll(disconnectCheckInterval);
        if (records.isEmpty()) {
          out.write("\n".getBytes(StandardCharsets.UTF_8));
          out.flush();
        } else {
          final List<String> values = formatter.format(records);
          for (final String value : values) {
            if (printFormat) {
              printFormat = false;
              out.write(("Format:" + formatter.getFormat().name() + "\n")
                            .getBytes(StandardCharsets.UTF_8));
            }
            if (messagesWritten++ % interval == 0) {
              out.write(value.getBytes(StandardCharsets.UTF_8));
              out.flush();
            }
          }
        }
      }
    } catch (final EOFException exception) {
      // Connection terminated, we can stop writing
    } catch (final Exception exception) {
      log.error("Exception encountered while writing to output stream", exception);
      outputException(out, exception);
    } finally {
      topicConsumer.close();
    }
  }

  private void outputException(final OutputStream out, final Exception exception) {
    try {
      out.write(exception.getMessage().getBytes(StandardCharsets.UTF_8));
      out.write("\n".getBytes(StandardCharsets.UTF_8));
      out.flush();
    } catch (final IOException e) {
      log.debug("Client disconnected while attempting to write an error message");
    }
  }
}
