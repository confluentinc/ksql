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
import com.google.common.base.Preconditions;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.streams.materialization.Locator.KsqlNode;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.rest.entity.KsqlHostInfoEntity;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.confluent.ksql.rest.entity.TableRows;
import io.confluent.ksql.rest.server.execution.PullQueryExecutor;
import io.confluent.ksql.rest.server.execution.PullQueryResult;
import io.confluent.ksql.rest.server.resources.streaming.Flow.Subscriber;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

class PullQueryPublisher implements Flow.Publisher<Collection<StreamedRow>> {

  private final ServiceContext serviceContext;
  private final ConfiguredStatement<Query> query;
  private final PullQueryExecutor pullQueryExecutor;
  private final long startTimeNanos;

  @VisibleForTesting
  PullQueryPublisher(
      final ServiceContext serviceContext,
      final ConfiguredStatement<Query> query,
      final PullQueryExecutor pullQueryExecutor,
      final long startTimeNanos
  ) {
    this.serviceContext = requireNonNull(serviceContext, "serviceContext");
    this.query = requireNonNull(query, "query");
    this.pullQueryExecutor = requireNonNull(pullQueryExecutor, "pullQueryExecutor");
    this.startTimeNanos = startTimeNanos;
  }

  @Override
  public synchronized void subscribe(final Subscriber<Collection<StreamedRow>> subscriber) {
    final PullQuerySubscription subscription = new PullQuerySubscription(
        subscriber,
        () -> pullQueryExecutor.execute(query, serviceContext, Optional.of(false), startTimeNanos)
    );

    subscriber.onSubscribe(subscription);
  }

  private static final class PullQuerySubscription implements Flow.Subscription {

    private final Subscriber<Collection<StreamedRow>> subscriber;
    private final Callable<PullQueryResult> executor;
    private boolean done = false;

    private PullQuerySubscription(
        final Subscriber<Collection<StreamedRow>> subscriber,
        final Callable<PullQueryResult> executor
    ) {
      this.subscriber = requireNonNull(subscriber, "subscriber");
      this.executor = requireNonNull(executor, "executor");
    }

    @Override
    public void request(final long n) {
      Preconditions.checkArgument(n == 1, "number of requested items must be 1");

      if (done) {
        return;
      }

      done = true;

      try {
        final PullQueryResult result = executor.call();
        final TableRows entity = result.getTableRows();
        final Optional<KsqlHostInfoEntity> host = result.getSourceNode()
            .map(KsqlNode::location)
            .map(location -> new KsqlHostInfoEntity(location.getHost(), location.getPort()));

        subscriber.onSchema(entity.getSchema());

        final List<StreamedRow> rows = entity.getRows().stream()
            .map(PullQuerySubscription::toGenericRow)
            .map(row -> StreamedRow.row(row, host))
            .collect(Collectors.toList());

        subscriber.onNext(rows);
        subscriber.onComplete();
      } catch (final Exception e) {
        subscriber.onError(e);
      }
    }

    @Override
    public void cancel() {
    }

    private static GenericRow toGenericRow(final List<?> values) {
      return new GenericRow().appendAll(values);
    }
  }
}
