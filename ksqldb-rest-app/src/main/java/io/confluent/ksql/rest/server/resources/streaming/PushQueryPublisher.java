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

import static java.util.Objects.requireNonNull;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.api.server.MetricsCallbackHolder;
import io.confluent.ksql.rest.entity.PushContinuationToken;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.confluent.ksql.rest.server.resources.streaming.Flow.Subscriber;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.LogicalSchema.Builder;
import io.confluent.ksql.util.KeyValueMetadata;
import io.confluent.ksql.util.PushQueryMetadata;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("UnstableApiUsage")
final class PushQueryPublisher implements Flow.Publisher<Collection<StreamedRow>> {

  private static final Logger log = LoggerFactory.getLogger(PushQueryPublisher.class);

  private final ListeningScheduledExecutorService exec;
  private final PushQueryMetadata queryMetadata;
  private final MetricsCallbackHolder metricsCallbackHolder;
  private final long startTimeNanos;

  PushQueryPublisher(
      final ListeningScheduledExecutorService exec,
      final PushQueryMetadata queryMetadata,
      final MetricsCallbackHolder metricsCallbackHolder,
      final long startTimeNanos
  ) {
    this.exec = exec;
    this.queryMetadata = queryMetadata;
    this.metricsCallbackHolder = metricsCallbackHolder;
    this.startTimeNanos = startTimeNanos;
  }

  @Override
  public synchronized void subscribe(final Flow.Subscriber<Collection<StreamedRow>> subscriber) {

    final PushQuerySubscription subscription =
        new PushQuerySubscription(exec, subscriber, queryMetadata);

    log.info("Running query {}", queryMetadata.getQueryId().toString());
    queryMetadata.start();

    final WebSocketSubscriber<StreamedRow> webSocketSubscriber =
        (WebSocketSubscriber<StreamedRow>) subscriber;

    webSocketSubscriber.onSubscribe(subscription, metricsCallbackHolder, startTimeNanos);
  }

  static class PushQuerySubscription extends PollingSubscription<Collection<StreamedRow>> {

    private final PushQueryMetadata queryMetadata;
    private boolean closed = false;

    PushQuerySubscription(
        final ListeningScheduledExecutorService exec,
        final Subscriber<Collection<StreamedRow>> subscriber,
        final PushQueryMetadata queryMetadata
    ) {
      super(exec, subscriber, valueColumnOnly(queryMetadata.getLogicalSchema()));
      this.queryMetadata = requireNonNull(queryMetadata, "queryMetadata");

      queryMetadata.setLimitHandler(this::setDone);
      queryMetadata.setCompletionHandler(this::setDone);
      queryMetadata.setUncaughtExceptionHandler(
          e -> {
            setError(e);
            return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
          }
      );
    }

    @Override
    public Collection<StreamedRow> poll() {
      final List<KeyValueMetadata<List<?>, GenericRow>> rows = Lists.newLinkedList();
      queryMetadata.getRowQueue().drainTo(rows);
      if (rows.isEmpty()) {
        return null;
      } else {
        return rows.stream()
            .map(kv -> {
              if (kv.getRowMetadata().isPresent()
                  && kv.getRowMetadata().get().getPushOffsetsRange().isPresent()) {
                return StreamedRow.continuationToken(new PushContinuationToken(
                    kv.getRowMetadata().get().getPushOffsetsRange().get().serialize()));
              } else {
                return StreamedRow.pushRow(kv.getKeyValue().value());
              }
            })
            .collect(Collectors.toCollection(Lists::newLinkedList));
      }
    }

    @Override
    public synchronized void close() {
      if (!closed) {
        closed = true;
        log.info("Terminating query {}", queryMetadata.getQueryId().toString());
        queryMetadata.close();
      }
    }
  }

  private static LogicalSchema valueColumnOnly(final LogicalSchema logicalSchema) {
    // Push queries only return value columns, but query metadata schema includes key and meta:
    final Builder builder = LogicalSchema.builder();
    logicalSchema.value().forEach(builder::valueColumn);
    return builder.build();
  }
}
