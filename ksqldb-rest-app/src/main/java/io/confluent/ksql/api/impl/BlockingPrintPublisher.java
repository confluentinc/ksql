/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.api.impl;

import io.confluent.ksql.api.server.KsqlApiException;
import io.confluent.ksql.parser.tree.PrintTopic;
import io.confluent.ksql.reactive.BasePublisher;
import io.confluent.ksql.rest.Errors;
import io.confluent.ksql.rest.server.resources.streaming.PrintTopicUtil;
import io.confluent.ksql.rest.server.resources.streaming.RecordFormatter;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.VertxUtils;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.WorkerExecutor;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.utils.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A query publisher that uses an internal blocking queue to store rows for delivery. It's currently
 * necessary to use a blocking queue as Kafka Streams delivers message in a synchronous fashion with
 * no back pressure. If the queue was not blocking then if the subscriber was slow the messages
 * could build up on the queue eventually resulting in out of memory. The only mechanism we have to
 * slow streams down is to block the thread. Kafka Streams uses dedicated streams per topology so
 * this won't prevent the thread from doing useful work elsewhere but it does mean we can't have too
 * many push queries in the server at any one time as we can end up with a lot of threads.
 */
public class BlockingPrintPublisher extends BasePublisher<String> {

  private static final Logger log = LoggerFactory.getLogger(BlockingPrintPublisher.class);

  public static final int SEND_MAX_BATCH_SIZE = 200;

  private final WorkerExecutor workerExecutor;
  private final KafkaConsumer<Bytes, Bytes> topicConsumer;
  private final PrintTopic printTopic;
  private final int limit;
  private final long interval;
  private final RecordFormatter formatter;
  private final Duration disconnectCheckInterval;
  private volatile boolean closed;
  private volatile boolean started;

  public BlockingPrintPublisher(final Context ctx,
      final WorkerExecutor workerExecutor,
      final ServiceContext serviceContext,
      final Map<String, Object> consumerProperties,
      final PrintTopic printTopic,
      final Duration disconnectCheckInterval) {
    super(ctx);
    this.workerExecutor = Objects.requireNonNull(workerExecutor);
    this.printTopic = printTopic;
    this.limit = printTopic.getLimit().orElse(Integer.MAX_VALUE);
    this.interval = printTopic.getIntervalValue();
    this.formatter = new RecordFormatter(
        serviceContext.getSchemaRegistryClient(),
        printTopic.getTopic()
    );
    this.disconnectCheckInterval = disconnectCheckInterval;
    this.topicConsumer = createTopicConsumer(serviceContext, consumerProperties, printTopic);
  }

  public Future<Void> close() {
    if (closed) {
      return Future.succeededFuture();
    }
    closed = true;
    // Run async as it can block
    ctx.runOnContext(v -> sendComplete());
    executeOnWorker(topicConsumer::close);

    return super.close();
  }


  @Override
  protected void maybeSend() {
    ctx.runOnContext(v -> {
      ctx.executeBlocking(t -> doSend());
    });
  }

  @Override
  protected void afterSubscribe() {
    if (!started) {
      started = true;
    }
  }

  public void startFromWorkerThread() {
    VertxUtils.checkIsWorker();
    started = true;
  }

  private void executeOnWorker(final Runnable runnable) {
    workerExecutor.executeBlocking(p -> runnable.run(), false, ar -> {
      if (ar.failed()) {
        log.error("Failed to close print", ar.cause());
      }
    });
  }

  private void doSend() {
    int messagesWritten = 0;
    int messagesPolled = 0;

    while (!isClosed() && getDemand() > 0 && messagesWritten < limit) {
      final ConsumerRecords<Bytes, Bytes> records = topicConsumer.poll(disconnectCheckInterval);
      if (records.isEmpty()) {
        continue;
      }

      final List<String> formattedRecords = formatter.format(
          records.records(this.printTopic.getTopic())
      );
      final List<String> formattedRecordsOld = formatConsumerRecords(records);

      for (final String message : formattedRecords) {
        if (messagesPolled++ % interval == 0) {
          messagesWritten++;
          doOnNext(message);
        }

        if (messagesWritten >= limit) {
          ctx.runOnContext(v -> sendComplete());
          break;
        } else if (messagesWritten >= SEND_MAX_BATCH_SIZE) {
          ctx.runOnContext(v -> doSend());
          break;
        }
      }
    }
  }

  private KafkaConsumer<Bytes, Bytes> createTopicConsumer(final ServiceContext serviceContext,
      final Map<String, Object> consumerProperties,
      final PrintTopic printTopic) {
    try {
      boolean topicExists = serviceContext.getAdminClient().listTopics().names().get().stream()
          .anyMatch(topicName -> topicName.equalsIgnoreCase(printTopic.getTopic()));

      if (!topicExists) {
        throw new KsqlApiException("KsqlException Topic does not exist: " + printTopic.getTopic(),
            Errors.ERROR_CODE_BAD_STATEMENT);
      }
    } catch (InterruptedException | ExecutionException e) {
      throw new KsqlException("Could not list existing kafka topics" + e);
    }

    return PrintTopicUtil.createTopicConsumer(serviceContext, consumerProperties, printTopic);
  }


  private List<String> formatConsumerRecords(final ConsumerRecords<Bytes, Bytes> records) {
    return StreamSupport.stream(records.spliterator(), false)
        .map(this::formatConsumerRecord)
        .collect(Collectors.toList());
  }

  private String formatConsumerRecord(final ConsumerRecord<Bytes, Bytes> record) {
    final String key = record.key() == null ? null : record.key().toString();
    final String value = record.value() == null ? null : record.value().toString();
    final String timestamp = record.timestamp() == RecordBatch.NO_TIMESTAMP
        ? null
        : Instant.ofEpochMilli(record.timestamp()).toString();
    return String.format(
        "rowtime: %s, key: %s, value: %s, partition: %d",
        timestamp,
        key,
        value,
        record.partition()
    );
  }

  private boolean isClosed() {
    return closed;
  }

}
