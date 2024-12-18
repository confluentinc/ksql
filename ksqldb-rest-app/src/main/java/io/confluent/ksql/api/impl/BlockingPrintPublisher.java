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
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.VertxUtils;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.WorkerExecutor;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.utils.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A publisher that creates a kafka consumer, consumes a topic for as long as necessary and
 * publishes the records. It's blocking since the doSend method for fetching records has a possibly
 * infinite while loop and will poll the topic indefinitely until the connection is closed if the
 * print statement has no LIMIT clause. We use vertx executeBlocking so the doSend function is
 * safely executed using a thread from the worker pool.
 */
public class BlockingPrintPublisher extends BasePublisher<String> {

  private static final Logger log = LoggerFactory.getLogger(BlockingPrintPublisher.class);
  private static final Duration POLL_TIMEOUT = Duration.ofMillis(5000L);
  public static final int SEND_MAX_BATCH_SIZE = 200;

  private final WorkerExecutor workerExecutor;
  private final KafkaConsumer<Bytes, Bytes> topicConsumer;
  private final PrintTopic printTopic;
  private final int limit;
  private final long interval;
  private final RecordFormatter formatter;
  private volatile boolean closed;
  private volatile boolean started;

  public BlockingPrintPublisher(final Context ctx,
      final WorkerExecutor workerExecutor,
      final ServiceContext serviceContext,
      final KsqlConfig ksqlConfig,
      final Map<String, Object> consumerProperties,
      final PrintTopic printTopic) {
    super(ctx);
    this.workerExecutor = Objects.requireNonNull(workerExecutor);
    this.printTopic = printTopic;
    this.limit = printTopic.getLimit().orElse(Integer.MAX_VALUE);
    this.interval = printTopic.getIntervalValue();
    this.formatter = new RecordFormatter(
        serviceContext.getSchemaRegistryClient(),
        printTopic.getTopic()
    );
    this.topicConsumer = createTopicConsumer(serviceContext, ksqlConfig, consumerProperties,
        printTopic);
  }

  public Future<Void> close() {
    if (closed) {
      return Future.succeededFuture();
    }
    closed = true;
    // Run async as it can block
    ctx.runOnContext(v -> sendComplete());

    return super.close();
  }


  @Override
  protected void maybeSend() {
    executeOnWorker(this::doSend);
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
    VertxUtils.checkIsWorker();
    int messagesWritten = 0;
    int messagesPolled = 0;

    while (!isClosed() && getDemand() > 0 && messagesWritten < limit) {
      final ConsumerRecords<Bytes, Bytes> records = topicConsumer.poll(POLL_TIMEOUT);
      if (records.isEmpty()) {
        continue;
      }
      final List<String> formattedRecords = formatter.format(
          records.records(this.printTopic.getTopic())
      );

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

    if (isClosed()) {
      topicConsumer.close();
    }
  }

  private KafkaConsumer<Bytes, Bytes> createTopicConsumer(final ServiceContext serviceContext,
      final KsqlConfig ksqlConfig,
      final Map<String, Object> consumerProperties,
      final PrintTopic printTopic) {
    try {
      final boolean topicExists = serviceContext.getAdminClient().listTopics().names().get()
          .stream().anyMatch(topicName -> topicName.equalsIgnoreCase(printTopic.getTopic()));

      if (!topicExists) {
        throw new KsqlApiException("Topic does not exist: " + printTopic.getTopic(),
            Errors.ERROR_CODE_BAD_STATEMENT);
      }
    } catch (InterruptedException | ExecutionException e) {
      throw new KsqlException("Could not list existing kafka topics" + e);
    }

    final Map<String, Object> ksqlStreamConfigProps = overrideDefaultKsqlStreamConfigProps(
        ksqlConfig);

    final Map<String, Object> finalConsumerProperties = populateKsqlStreamConfigProps(
        ksqlStreamConfigProps,
        consumerProperties);
    return PrintTopicUtil.createTopicConsumer(serviceContext, finalConsumerProperties,
        printTopic);
  }

  private Map<String, Object> populateKsqlStreamConfigProps(final Map<String, Object> ksqlConfig,
      final Map<String, Object> properties) {
    final Map<String, Object> consumerProperties = new HashMap<>(ksqlConfig);
    consumerProperties.putAll(properties);

    return consumerProperties;
  }

  private Map<String, Object> overrideDefaultKsqlStreamConfigProps(final KsqlConfig ksqlConfig) {
    final Map<String, Object> overriddenProperties = new HashMap<>(
        ksqlConfig.getKsqlStreamConfigProps());
    // We override the default value of auto.offset.reset to latest because that's the default
    // behavior for the print topic command, unlike the default behavior for push and pull queries.
    overriddenProperties.put("auto.offset.reset", "latest");

    return overriddenProperties;
  }

  private boolean isClosed() {
    return closed;
  }

}
