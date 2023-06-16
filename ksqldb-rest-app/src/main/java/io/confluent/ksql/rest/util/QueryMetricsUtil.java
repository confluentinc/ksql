/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.rest.util;

import io.confluent.ksql.analyzer.ImmutableAnalysis;
import io.confluent.ksql.api.server.MetricsCallback;
import io.confluent.ksql.api.server.SlidingWindowRateLimiter;
import io.confluent.ksql.execution.pull.PullPhysicalPlan.PullPhysicalPlanType;
import io.confluent.ksql.execution.pull.PullQueryResult;
import io.confluent.ksql.internal.PullQueryExecutorMetrics;
import io.confluent.ksql.internal.ScalablePushQueryMetrics;
import io.confluent.ksql.query.TransientQueryQueue;
import io.confluent.ksql.rest.util.ConcurrencyLimiter.Decrementer;
import io.confluent.ksql.util.KsqlConstants.QuerySourceType;
import io.confluent.ksql.util.KsqlConstants.RoutingNodeType;
import io.confluent.ksql.util.ScalablePushQueryMetadata;
import io.confluent.ksql.util.StreamPullQueryMetadata;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.State;

public final class QueryMetricsUtil {

  private QueryMetricsUtil() {
  }

  public static MetricsCallback initializePullTableMetricsCallback(
      final Optional<PullQueryExecutorMetrics> pullQueryMetrics,
      final SlidingWindowRateLimiter pullBandRateLimiter,
      final AtomicReference<PullQueryResult> resultForMetrics) {

    final MetricsCallback metricsCallback =
        (statusCode, requestBytes, responseBytes, startTimeNanos) ->
            pullQueryMetrics.ifPresent(metrics -> {
              metrics.recordStatusCode(statusCode);
              metrics.recordRequestSize(requestBytes);

              final PullQueryResult r = resultForMetrics.get();
              if (r == null) {
                recordErrorMetrics(pullQueryMetrics, responseBytes, startTimeNanos);
              } else {
                final QuerySourceType sourceType = r.getSourceType();
                final PullPhysicalPlanType planType = r.getPlanType();
                final RoutingNodeType routingNodeType = RoutingNodeType.SOURCE_NODE;
                metrics.recordResponseSize(
                    responseBytes,
                    sourceType,
                    planType,
                    routingNodeType
                );
                metrics.recordLatency(
                    startTimeNanos,
                    sourceType,
                    planType,
                    routingNodeType
                );
                metrics.recordRowsReturned(
                    r.getTotalRowsReturned(),
                    sourceType, planType, routingNodeType);
                metrics.recordRowsProcessed(
                    r.getTotalRowsProcessed(),
                    sourceType, planType, routingNodeType);
              }
              pullBandRateLimiter.add(responseBytes);
            });

    return metricsCallback;
  }

  public static MetricsCallback initializePullStreamMetricsCallback(
      final Optional<PullQueryExecutorMetrics> pullQueryMetrics,
      final SlidingWindowRateLimiter pullBandRateLimiter,
      final ImmutableAnalysis analysis,
      final AtomicReference<StreamPullQueryMetadata> resultForMetrics,
      final AtomicReference<Decrementer> refDecrementer) {

    final MetricsCallback metricsCallback =
        (statusCode, requestBytes, responseBytes, startTimeNanos) ->
            pullQueryMetrics.ifPresent(metrics -> {
              metrics.recordStatusCode(statusCode);
              metrics.recordRequestSize(requestBytes);

              final StreamPullQueryMetadata m = resultForMetrics.get();
              final KafkaStreams.State state = m == null ? null : m.getTransientQueryMetadata()
                  .getKafkaStreams().state();

              if (m == null || state == null
                  || state.equals(State.ERROR)
                  || state.equals(State.PENDING_ERROR)) {
                recordErrorMetrics(pullQueryMetrics, responseBytes, startTimeNanos);
              } else {
                final boolean isWindowed = analysis
                    .getFrom()
                    .getDataSource()
                    .getKsqlTopic()
                    .getKeyFormat().isWindowed();
                final QuerySourceType sourceType = isWindowed
                    ? QuerySourceType.WINDOWED_STREAM : QuerySourceType.NON_WINDOWED_STREAM;
                // There is no WHERE clause constraint information in the persistent logical plan
                final PullPhysicalPlanType planType = PullPhysicalPlanType.UNKNOWN;
                final RoutingNodeType routingNodeType = RoutingNodeType.SOURCE_NODE;
                metrics.recordResponseSize(
                    responseBytes,
                    sourceType,
                    planType,
                    routingNodeType
                );
                metrics.recordLatency(
                    startTimeNanos,
                    sourceType,
                    planType,
                    routingNodeType
                );
                final TransientQueryQueue rowQueue = (TransientQueryQueue)
                    m.getTransientQueryMetadata().getRowQueue();
                // The rows read from the underlying data source equal the rows read by the user
                // since the WHERE condition is pushed to the data source
                metrics.recordRowsReturned(rowQueue.getTotalRowsQueued(), sourceType, planType,
                                           routingNodeType);
                metrics.recordRowsProcessed(rowQueue.getTotalRowsQueued(), sourceType, planType,
                                            routingNodeType);
              }
              pullBandRateLimiter.add(responseBytes);
              // Decrement on happy or exception path
              final Decrementer decrementer = refDecrementer.get();
              if (decrementer != null) {
                decrementer.decrementAtMostOnce();
              }
            });

    return metricsCallback;
  }

  public static MetricsCallback initializeScalablePushMetricsCallback(
          final Optional<ScalablePushQueryMetrics> scalablePushQueryMetrics,
          final SlidingWindowRateLimiter scalablePushBandRateLimiter,
          final AtomicReference<ScalablePushQueryMetadata> resultForMetrics) {

    final MetricsCallback metricsCallback =
            (statusCode, requestBytes, responseBytes, startTimeNanos) ->
                    scalablePushQueryMetrics.ifPresent(metrics -> {
                      metrics.recordStatusCode(statusCode);
                      metrics.recordRequestSize(requestBytes);
                      final ScalablePushQueryMetadata r = resultForMetrics.get();
                      if (r == null) {
                        metrics.recordResponseSizeForError(responseBytes);
                        metrics.recordConnectionDurationForError(startTimeNanos);
                        metrics.recordZeroRowsReturnedForError();
                        metrics.recordZeroRowsProcessedForError();
                      } else {
                        final QuerySourceType sourceType = r.getSourceType();
                        final RoutingNodeType routingNodeType = r.getRoutingNodeType();
                        metrics.recordResponseSize(
                                responseBytes,
                                sourceType,
                                routingNodeType
                        );
                        metrics.recordConnectionDuration(
                                startTimeNanos,
                                sourceType,
                                routingNodeType
                        );
                        metrics.recordRowsReturned(
                                r.getTotalRowsReturned(),
                                sourceType, routingNodeType);
                        metrics.recordRowsProcessed(
                                r.getTotalRowsProcessed(),
                                sourceType, routingNodeType);
                      }
                      scalablePushBandRateLimiter.add(responseBytes);
                    });

    return metricsCallback;
  }

  private static void recordErrorMetrics(
      final Optional<PullQueryExecutorMetrics> pullQueryMetrics,
      final long responseBytes,
      final long startTimeNanos) {
    pullQueryMetrics.ifPresent(metrics -> {
      metrics.recordResponseSizeForError(responseBytes);
      metrics.recordLatencyForError(startTimeNanos);
      metrics.recordZeroRowsReturnedForError();
      metrics.recordZeroRowsProcessedForError();
    });
  }

}
