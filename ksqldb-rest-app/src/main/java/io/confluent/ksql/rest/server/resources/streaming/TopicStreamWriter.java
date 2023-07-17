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
import io.confluent.ksql.api.server.StreamingOutput;
import io.confluent.ksql.parser.tree.PrintTopic;
import io.confluent.ksql.services.ServiceContext;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.utils.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TopicStreamWriter implements StreamingOutput {

  private static final Logger log = LoggerFactory.getLogger(TopicStreamWriter.class);
  private static final int WRITE_TIMEOUT_MS = 10 * 60000;
  private final long interval;
  private final Duration disconnectCheckInterval;
  private final KafkaConsumer<Bytes, Bytes> topicConsumer;
  private final SchemaRegistryClient schemaRegistryClient;
  private final String topicName;
  private final Predicate<Long> limitReached;
  private long messagesWritten;
  private long messagesPolled;
  private volatile boolean connectionClosed;
  private boolean closed;

  public static TopicStreamWriter create(
      final ServiceContext serviceContext,
      final Map<String, Object> consumerProperties,
      final PrintTopic printTopic,
      final Duration disconnectCheckInterval,
      final CompletableFuture<Void> connectionClosedFuture
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
        printTopic.getLimit(),
        connectionClosedFuture);
  }

  TopicStreamWriter(
      final SchemaRegistryClient schemaRegistryClient,
      final KafkaConsumer<Bytes, Bytes> topicConsumer,
      final String topicName,
      final long interval,
      final Duration disconnectCheckInterval,
      final OptionalInt limit,
      final CompletableFuture<Void> connectionClosedFuture
  ) {
    this.topicConsumer = requireNonNull(topicConsumer, "topicConsumer");
    this.schemaRegistryClient = requireNonNull(schemaRegistryClient, "schemaRegistryClient");
    this.topicName = requireNonNull(topicName, "topicName");
    this.interval = interval;
    this.limitReached = requireNonNull(limit, "limit").isPresent()
        ? written -> written >= limit.getAsInt()
        : written -> false;
    this.disconnectCheckInterval =
        requireNonNull(disconnectCheckInterval, "disconnectCheckInterval");
    this.messagesWritten = 0;
    this.messagesPolled = 0;
    connectionClosedFuture.thenAccept(v -> connectionClosed = true);

    if (interval < 1) {
      throw new IllegalArgumentException("INTERVAL must be greater than one, but was: " + interval);
    }
  }

  @Override
  public void write(final OutputStream out) {
    try {
      final PrintStream print = new PrintStream(out, true, "UTF8");
      final RecordFormatter formatter = new RecordFormatter(schemaRegistryClient, topicName);

      final FormatsTracker formatsTracker = new FormatsTracker(print);
      while (!connectionClosed && !print.checkError() && !limitReached.test(messagesWritten)) {
        final ConsumerRecords<Bytes, Bytes> records = topicConsumer.poll(disconnectCheckInterval);
        if (records.isEmpty()) {
          print.println();
          continue;
        }

        final List<String> values = formatter.format(records.records(topicName));
        if (values.isEmpty()) {
          continue;
        }

        final List<String> toOutput = new ArrayList<>();
        for (final String value : values) {
          if (messagesPolled++ % interval == 0) {
            messagesWritten++;
            toOutput.add(value);
          }

          if (limitReached.test(messagesWritten)) {
            break;
          }
        }

        formatsTracker.update(formatter);

        toOutput.forEach(print::println);
      }
    } catch (final Exception exception) {
      log.error("Exception encountered while writing to output stream", exception);
      outputException(out, exception);
    } finally {
      close();
    }
  }

  @Override
  public synchronized void close() {
    if (!closed) {
      topicConsumer.close();
      closed = true;
    }
  }

  @Override
  public int getWriteTimeoutMs() {
    return WRITE_TIMEOUT_MS;
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

  private static final class FormatsTracker {

    private final PrintStream out;
    private final List<String> keyFormats = new ArrayList<>();
    private final List<String> valueFormats = new ArrayList<>();

    FormatsTracker(final PrintStream out) {
      this.out = requireNonNull(out, "out");
      this.keyFormats.add("add an entry to force output for formats on the first loop");
      this.valueFormats.add("add an entry to force output for formats on the first loop");
    }

    public void update(final RecordFormatter formatter) {
      update(out, keyFormats, formatter.getPossibleKeyFormats(), "Key format: ");
      update(out, valueFormats, formatter.getPossibleValueFormats(), "Value format: ");
    }

    private static void update(
        final PrintStream out,
        final List<String> previous,
        final List<String> current,
        final String prefix
    ) {
      if (previous.equals(current)) {
        return;
      }

      previous.clear();
      previous.addAll(current);

      out.print(prefix);

      if (current.isEmpty()) {
        out.println(" does not match any supported format. "
            + "It may be a STRING with encoding other than UTF8, or some other format.");
      } else {
        out.println(String.join(" or ", current));
      }
    }
  }
}
