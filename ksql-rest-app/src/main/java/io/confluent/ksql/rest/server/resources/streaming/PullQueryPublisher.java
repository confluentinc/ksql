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
import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.confluent.ksql.rest.entity.TableRowsEntity;
import io.confluent.ksql.rest.server.execution.PullQueryExecutor;
import io.confluent.ksql.rest.server.resources.streaming.Flow.Subscriber;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

class PullQueryPublisher implements Flow.Publisher<Collection<StreamedRow>> {

  private final KsqlEngine ksqlEngine;
  private final ServiceContext serviceContext;
  private final ConfiguredStatement<Query> query;
  private final TheQueryExecutor pullQueryExecutor;

  PullQueryPublisher(
      final KsqlEngine ksqlEngine,
      final ServiceContext serviceContext,
      final ConfiguredStatement<Query> query
  ) {
    this(ksqlEngine, serviceContext, query, PullQueryExecutor::execute);
  }

  @VisibleForTesting
  PullQueryPublisher(
      final KsqlEngine ksqlEngine,
      final ServiceContext serviceContext,
      final ConfiguredStatement<Query> query,
      final TheQueryExecutor pullQueryExecutor
  ) {
    this.ksqlEngine = requireNonNull(ksqlEngine, "ksqlEngine");
    this.serviceContext = requireNonNull(serviceContext, "serviceContext");
    this.query = requireNonNull(query, "query");
    this.pullQueryExecutor = requireNonNull(pullQueryExecutor, "pullQueryExecutor");
  }

  @Override
  public synchronized void subscribe(final Subscriber<Collection<StreamedRow>> subscriber) {
    final PullQuerySubscription subscription = new PullQuerySubscription(
        subscriber,
        () -> pullQueryExecutor.execute(query, ksqlEngine, serviceContext)
    );

    subscriber.onSubscribe(subscription);
  }

  private static final class PullQuerySubscription implements Flow.Subscription {

    private final Subscriber<Collection<StreamedRow>> subscriber;
    private final Callable<TableRowsEntity> executor;
    private boolean done = false;

    private PullQuerySubscription(
        final Subscriber<Collection<StreamedRow>> subscriber,
        final Callable<TableRowsEntity> executor
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
        final TableRowsEntity entity = executor.call();

        subscriber.onSchema(entity.getSchema());

        final List<StreamedRow> rows = entity.getRows().stream()
            .map(PullQuerySubscription::toGenericRow)
            .map(StreamedRow::row)
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

    @SuppressWarnings("unchecked")
    private static GenericRow toGenericRow(final List<?> values) {
      return new GenericRow((List)values);
    }
  }

  interface TheQueryExecutor {

    TableRowsEntity execute(
        ConfiguredStatement<Query> statement,
        KsqlExecutionContext executionContext,
        ServiceContext serviceContext
    );
  }
}
