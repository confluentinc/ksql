/*
 * Copyright 2019 Confluent Inc.
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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.api.server.MetricsCallbackHolder;
import io.confluent.ksql.execution.pull.PullQueryResult;
import io.confluent.ksql.rest.entity.ConsistencyToken;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.confluent.ksql.rest.server.resources.streaming.Flow.Subscriber;
import io.confluent.ksql.util.KeyValueMetadata;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class PullQueryPublisher implements Flow.Publisher<Collection<StreamedRow>> {
  private static final Logger LOG = LoggerFactory.getLogger(PullQueryPublisher.class);

  private final ListeningScheduledExecutorService exec;
  private final PullQueryResult result;
  private final MetricsCallbackHolder metricsCallbackHolder;
  private final long startTimeNanos;

  @VisibleForTesting
  PullQueryPublisher(
      final ListeningScheduledExecutorService exec,
      final PullQueryResult result,
      final MetricsCallbackHolder metricsCallbackHolder,
      final long startTimeNanos
  ) {
    this.exec = requireNonNull(exec, "exec");
    this.result = requireNonNull(result, "result");
    this.metricsCallbackHolder = metricsCallbackHolder;
    this.startTimeNanos = startTimeNanos;
  }

  @Override
  public synchronized void subscribe(final Subscriber<Collection<StreamedRow>> subscriber) {
    final PullQuerySubscription subscription = new PullQuerySubscription(
        exec, subscriber, result);

    result.start();

    final WebSocketSubscriber<StreamedRow> webSocketSubscriber =
        (WebSocketSubscriber<StreamedRow>) subscriber;

    webSocketSubscriber.onSubscribe(subscription, metricsCallbackHolder, startTimeNanos);
  }

  private static final class PullQuerySubscription
      extends PollingSubscription<Collection<StreamedRow>> {

    private final Subscriber<Collection<StreamedRow>> subscriber;
    private final PullQueryResult result;

    private PullQuerySubscription(
        final ListeningScheduledExecutorService exec,
        final Subscriber<Collection<StreamedRow>> subscriber,
        final PullQueryResult result
    ) {
      super(exec, subscriber, result.getSchema());
      this.subscriber = requireNonNull(subscriber, "subscriber");
      this.result = requireNonNull(result, "result");

      result.onCompletion(v -> {
        result.getConsistencyOffsetVector().ifPresent(
            result.getPullQueryQueue()::putConsistencyVector);
        setDone();
      });
      result.onException(this::setError);
    }

    @Override
    Collection<StreamedRow> poll() {
      final List<KeyValueMetadata<List<?>, GenericRow>> rows = Lists.newLinkedList();
      result.getPullQueryQueue().drainTo(rows);
      if (rows.isEmpty()) {
        return null;
      } else {
        return rows.stream()
            .map(kv -> {
              if (kv.getRowMetadata().isPresent()
                  && kv.getRowMetadata().get().getConsistencyOffsetVector().isPresent()) {
                return StreamedRow.consistencyToken(new ConsistencyToken(
                    kv.getRowMetadata().get().getConsistencyOffsetVector().get().serialize()));
              } else {
                return StreamedRow.pushRow(kv.getKeyValue().value());
              }
            })
            .collect(Collectors.toCollection(Lists::newLinkedList));
      }
    }

    @Override
    void close() {
      result.stop();
    }
  }
}
