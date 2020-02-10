/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.rest.server.resources.streaming;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.parser.tree.PrintTopic;
import io.confluent.ksql.rest.server.resources.streaming.TopicStream.RecordFormatter;
import io.confluent.ksql.services.ServiceContext;
import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.function.Supplier;
import javax.ws.rs.core.StreamingOutput;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.utils.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TopicStreamWriter implements StreamingOutput {

  private static final Logger log = LoggerFactory.getLogger(TopicStreamWriter.class);
  private final long interval;
  private final Duration disconnectCheckInterval;
  private final KafkaConsumer<Bytes, Bytes> topicConsumer;
  private final SchemaRegistryClient schemaRegistryClient;
  private final String topicName;
  private final OptionalInt limit;

  private long messagesWritten;
  private long messagesPolled;

  public static TopicStreamWriter create(
      final ServiceContext serviceContext,
      final Map<String, Object> consumerProperties,
      final PrintTopic printTopic,
      final Duration disconnectCheckInterval
  ) {
    return new TopicStreamWriter(
        serviceContext.getSchemaRegistryClient(),
        PrintTopicUtil.createTopicConsumer(
            serviceContext,
            consumerProperties,
            printTopic),
        printTopic.getTopic(),
        printTopic.getIntervalValue(),
        disconnectCheckInterval,
        printTopic.getLimit());
  }

  TopicStreamWriter(
      final SchemaRegistryClient schemaRegistryClient,
      final KafkaConsumer<Bytes, Bytes> topicConsumer,
      final String topicName,
      final long interval,
      final Duration disconnectCheckInterval,
      final OptionalInt limit
  ) {
    this.topicConsumer = requireNonNull(topicConsumer, "topicConsumer");
    this.schemaRegistryClient = requireNonNull(schemaRegistryClient, "schemaRegistryClient");
    this.topicName = requireNonNull(topicName, "topicName");
    this.interval = interval;
    this.limit = requireNonNull(limit, "limit");
    this.disconnectCheckInterval =
        requireNonNull(disconnectCheckInterval, "disconnectCheckInterval");
    this.messagesWritten = 0;
    this.messagesPolled = 0;

    if (interval < 1) {
      throw new IllegalArgumentException("INTERVAL must be greater than one, but was: " + interval);
    }
  }

  @Override
  public void write(final OutputStream out) {
    try {
      final RecordFormatter formatter = new RecordFormatter(schemaRegistryClient, topicName);

      boolean printFormat = true;
      while (true) {
        final ConsumerRecords<Bytes, Bytes> records = topicConsumer.poll(disconnectCheckInterval);
        if (records.isEmpty()) {
          out.write("\n".getBytes(UTF_8));
          out.flush();
          continue;
        }

        final List<Supplier<String>> values = formatter.format(records.records(topicName));
        for (final Supplier<String> value : values) {
          if (printFormat) {
            printFormat = false;
            out.write(("Key-Format:" + formatter.getKeyFormat().name() + "\n").getBytes(UTF_8));
            out.write(("Value-Format:" + formatter.getValueFormat().name() + "\n").getBytes(UTF_8));
          }

          if (messagesPolled++ % interval == 0) {
            messagesWritten++;
            out.write(value.get().getBytes(UTF_8));
            out.write(System.lineSeparator().getBytes(UTF_8));
            out.flush();
          }

          if (limit.isPresent() && messagesWritten >= limit.getAsInt()) {
            return;
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

  private static void outputException(final OutputStream out, final Exception exception) {
    try {
      out.write(exception.getMessage().getBytes(UTF_8));
      out.write("\n".getBytes(UTF_8));
      out.flush();
    } catch (final IOException e) {
      log.debug("Client disconnected while attempting to write an error message");
    }
  }
}
