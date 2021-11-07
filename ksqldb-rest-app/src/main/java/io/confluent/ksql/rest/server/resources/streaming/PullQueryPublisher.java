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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.RateLimiter;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.analyzer.ImmutableAnalysis;
import io.confluent.ksql.api.server.SlidingWindowRateLimiter;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.engine.PullQueryExecutionUtil;
import io.confluent.ksql.execution.streams.RoutingFilter.RoutingFilterFactory;
import io.confluent.ksql.execution.streams.RoutingOptions;
import io.confluent.ksql.internal.PullQueryExecutorMetrics;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.physical.pull.HARouting;
import io.confluent.ksql.physical.pull.PullPhysicalPlan.PullPhysicalPlanType;
import io.confluent.ksql.physical.pull.PullQueryResult;
import io.confluent.ksql.rest.entity.ConsistencyToken;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.confluent.ksql.rest.server.resources.streaming.Flow.Subscriber;
import io.confluent.ksql.rest.util.ConcurrencyLimiter;
import io.confluent.ksql.rest.util.ConcurrencyLimiter.Decrementer;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.ConsistencyOffsetVector;
import io.confluent.ksql.util.KeyValueMetadata;
import io.confluent.ksql.util.KsqlConstants.KsqlQueryType;
import io.confluent.ksql.util.KsqlConstants.QuerySourceType;
import io.confluent.ksql.util.KsqlConstants.RoutingNodeType;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

class PullQueryPublisher implements Flow.Publisher<Collection<StreamedRow>> {

  private final KsqlEngine ksqlEngine;
  private final ServiceContext serviceContext;
  private final ListeningScheduledExecutorService exec;
  private final ConfiguredStatement<Query> query;
  private final ImmutableAnalysis analysis;
  private final Optional<PullQueryExecutorMetrics> pullQueryMetrics;
  private final long startTimeNanos;
  private final RoutingFilterFactory routingFilterFactory;
  private final RateLimiter rateLimiter;
  private final ConcurrencyLimiter concurrencyLimiter;
  private final SlidingWindowRateLimiter pullBandRateLimiter;
  private final HARouting routing;
  private final Optional<ConsistencyOffsetVector> consistencyOffsetVector;

  @VisibleForTesting
  // CHECKSTYLE_RULES.OFF: ParameterNumberCheck
  PullQueryPublisher(
      // CHECKSTYLE_RULES.OFF: ParameterNumberCheck
      final KsqlEngine ksqlEngine,
      final ServiceContext serviceContext,
      final ListeningScheduledExecutorService exec,
      final ConfiguredStatement<Query> query,
      final ImmutableAnalysis analysis,
      final Optional<PullQueryExecutorMetrics> pullQueryMetrics,
      final long startTimeNanos,
      final RoutingFilterFactory routingFilterFactory,
      final RateLimiter rateLimiter,
      final ConcurrencyLimiter concurrencyLimiter,
      final SlidingWindowRateLimiter pullBandRateLimiter,
      final HARouting routing,
      final Optional<ConsistencyOffsetVector> consistencyOffsetVector
  ) {
    this.ksqlEngine = requireNonNull(ksqlEngine, "ksqlEngine");
    this.serviceContext = requireNonNull(serviceContext, "serviceContext");
    this.exec = requireNonNull(exec, "exec");
    this.query = requireNonNull(query, "query");
    this.analysis = requireNonNull(analysis, "analysis");
    this.pullQueryMetrics = pullQueryMetrics;
    this.startTimeNanos = startTimeNanos;
    this.routingFilterFactory = requireNonNull(routingFilterFactory, "routingFilterFactory");
    this.rateLimiter = requireNonNull(rateLimiter, "rateLimiter");
    this.concurrencyLimiter = concurrencyLimiter;
    this.pullBandRateLimiter = requireNonNull(pullBandRateLimiter, "pullBandRateLimiter");
    this.routing = requireNonNull(routing, "routing");
    this.consistencyOffsetVector = requireNonNull(
        consistencyOffsetVector, "consistencyOffsetVector");
  }

  @Override
  public synchronized void subscribe(final Subscriber<Collection<StreamedRow>> subscriber) {
    final RoutingOptions routingOptions = new PullQueryConfigRoutingOptions(
        query.getSessionConfig().getConfig(false),
        query.getSessionConfig().getOverrides(),
        ImmutableMap.of()
    );

    final PullQueryConfigPlannerOptions plannerOptions = new PullQueryConfigPlannerOptions(
        query.getSessionConfig().getConfig(false),
        query.getSessionConfig().getOverrides()
    );

    PullQueryResult result = null;
    Decrementer decrementer = null;
    try {
      PullQueryExecutionUtil.checkRateLimit(rateLimiter);
      decrementer = concurrencyLimiter.increment();
      pullBandRateLimiter.allow(KsqlQueryType.PULL);
      final Decrementer finalDecrementer = decrementer;

      result = ksqlEngine.executeTablePullQuery(
          analysis,
          serviceContext,
          query,
          routing,
          routingOptions,
          plannerOptions,
          pullQueryMetrics,
          true,
          consistencyOffsetVector
      );

      final PullQueryResult finalResult = result;
      result.onCompletionOrException((v, throwable) -> {
        finalDecrementer.decrementAtMostOnce();

        pullQueryMetrics.ifPresent(m -> {
          recordMetrics(m, finalResult);
        });
      });

      final PullQuerySubscription subscription = new PullQuerySubscription(
          exec, subscriber, result);

      subscriber.onSubscribe(subscription);
    } catch (Throwable t) {
      if (decrementer != null) {
        decrementer.decrementAtMostOnce();
      }

      if (result == null) {
        pullQueryMetrics.ifPresent(this::recordErrorMetrics);
      }
      throw t;
    }
  }

  private void recordMetrics(
      final PullQueryExecutorMetrics metrics, final PullQueryResult result) {

    final QuerySourceType sourceType = result.getSourceType();
    final PullPhysicalPlanType planType = result.getPlanType();
    final RoutingNodeType routingNodeType = result.getRoutingNodeType();
    // Note: we are not recording response size in this case because it is not
    // accessible in the websocket endpoint.
    metrics.recordLatency(startTimeNanos, sourceType, planType, routingNodeType);
    metrics.recordRowsReturned(result.getTotalRowsReturned(),
        sourceType, planType, routingNodeType);
    metrics.recordRowsProcessed(result.getTotalRowsProcessed(),
        sourceType, planType, routingNodeType);
  }

  private void recordErrorMetrics(final PullQueryExecutorMetrics metrics) {
    metrics.recordLatencyForError(startTimeNanos);
    metrics.recordZeroRowsReturnedForError();
    metrics.recordZeroRowsProcessedForError();
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
