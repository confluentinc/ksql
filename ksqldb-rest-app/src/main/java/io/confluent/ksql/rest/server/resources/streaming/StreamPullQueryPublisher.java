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
import io.confluent.ksql.logging.query.QueryLogger;
import io.confluent.ksql.metastore.model.DataSource.DataSourceType;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.physical.pull.HARouting;
import io.confluent.ksql.physical.pull.PullPhysicalPlan.PullPhysicalPlanType;
import io.confluent.ksql.physical.pull.PullPhysicalPlan.PullSourceType;
import io.confluent.ksql.physical.pull.PullPhysicalPlan.RoutingNodeType;
import io.confluent.ksql.physical.pull.PullQueryResult;
import io.confluent.ksql.physical.scalablepush.PushRoutingOptions;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.confluent.ksql.rest.server.LocalCommands;
import io.confluent.ksql.rest.server.resources.streaming.Flow.Subscriber;
import io.confluent.ksql.rest.server.resources.streaming.PushQueryPublisher.PushQuerySubscription;
import io.confluent.ksql.rest.util.ConcurrencyLimiter;
import io.confluent.ksql.rest.util.ConcurrencyLimiter.Decrementer;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KeyValue;
import io.confluent.ksql.util.PushQueryMetadata;
import io.confluent.ksql.util.ScalablePushQueryMetadata;
import io.confluent.ksql.util.TransientQueryMetadata;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class StreamPullQueryPublisher implements Flow.Publisher<Collection<StreamedRow>> {

  private static final Logger log = LoggerFactory.getLogger(StreamPullQueryPublisher.class);

  private final KsqlEngine ksqlEngine;
  private final ServiceContext serviceContext;
  private final ListeningScheduledExecutorService exec;
  private final ConfiguredStatement<Query> query;
  private final Optional<LocalCommands> localCommands;
  private final ImmutableAnalysis analysis;
  private final Optional<PullQueryExecutorMetrics> pullQueryMetrics;
  private final long startTimeNanos;
  private final RoutingFilterFactory routingFilterFactory;
  private final RateLimiter rateLimiter;
  private final ConcurrencyLimiter concurrencyLimiter;
  private final SlidingWindowRateLimiter pullBandRateLimiter;
  private final HARouting routing;

  @VisibleForTesting
    // CHECKSTYLE_RULES.OFF: ParameterNumberCheck
  StreamPullQueryPublisher(
      // CHECKSTYLE_RULES.OFF: ParameterNumberCheck
      final KsqlEngine ksqlEngine,
      final ServiceContext serviceContext,
      final ListeningScheduledExecutorService exec,
      final ConfiguredStatement<Query> query,
      final Optional<LocalCommands> localCommands,
      final ImmutableAnalysis analysis,
      final Optional<PullQueryExecutorMetrics> pullQueryMetrics,
      final long startTimeNanos,
      final RoutingFilterFactory routingFilterFactory,
      final RateLimiter rateLimiter,
      final ConcurrencyLimiter concurrencyLimiter,
      final SlidingWindowRateLimiter pullBandRateLimiter,
      final HARouting routing
  ) {
    this.ksqlEngine = requireNonNull(ksqlEngine, "ksqlEngine");
    this.serviceContext = requireNonNull(serviceContext, "serviceContext");
    this.exec = requireNonNull(exec, "exec");
    this.query = requireNonNull(query, "query");
    this.localCommands = requireNonNull(localCommands, "localCommands");
    this.analysis = requireNonNull(analysis, "analysis");
    this.pullQueryMetrics = pullQueryMetrics;
    this.startTimeNanos = startTimeNanos;
    this.routingFilterFactory = requireNonNull(routingFilterFactory, "routingFilterFactory");
    this.rateLimiter = requireNonNull(rateLimiter, "rateLimiter");
    this.concurrencyLimiter = concurrencyLimiter;
    this.pullBandRateLimiter = requireNonNull(pullBandRateLimiter, "pullBandRateLimiter");
    this.routing = requireNonNull(routing, "routing");
  }

  @Override
  public synchronized void subscribe(final Subscriber<Collection<StreamedRow>> subscriber) {

    PullQueryExecutionUtil.checkRateLimit(rateLimiter);
    final Decrementer decrementer = concurrencyLimiter.increment();
    pullBandRateLimiter.allow();

    final TransientQueryMetadata queryMetadata;
    final PushQuerySubscription subscription;

    try {
      queryMetadata = ksqlEngine
          .createStreamPullQuery(serviceContext, analysis, query, true);

      localCommands.ifPresent(lc -> lc.write(queryMetadata));

      subscription = new PushQuerySubscription(exec, subscriber, queryMetadata);

      log.info("Running query {}", queryMetadata.getQueryId().toString());
      QueryLogger.info("Running query", query.getStatement());
      queryMetadata.start();

      subscriber.onSubscribe(subscription);

      ksqlEngine.waitForStreamPullQuery(serviceContext, analysis, query, queryMetadata);

    } finally {
      decrementer.decrementAtMostOnce();

      if (subscription != null) {
        subscription.close();
      } else if (queryMetadata != null) {
        queryMetadata.close();
      }
    }

  }
}
