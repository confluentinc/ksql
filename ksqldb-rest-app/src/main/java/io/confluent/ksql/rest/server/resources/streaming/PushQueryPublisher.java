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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.analyzer.ImmutableAnalysis;
import io.confluent.ksql.api.server.SlidingWindowRateLimiter;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.internal.ScalablePushQueryMetrics;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.physical.scalablepush.PushRouting;
import io.confluent.ksql.physical.scalablepush.PushRoutingOptions;
import io.confluent.ksql.rest.entity.RowOffsets;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.confluent.ksql.rest.server.LocalCommands;
import io.confluent.ksql.rest.server.resources.streaming.Flow.Subscriber;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.LogicalSchema.Builder;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KeyValueMetadata;
import io.confluent.ksql.util.KsqlConstants.KsqlQueryType;
import io.confluent.ksql.util.KsqlConstants.QuerySourceType;
import io.confluent.ksql.util.KsqlConstants.RoutingNodeType;
import io.confluent.ksql.util.PushQueryMetadata;
import io.confluent.ksql.util.ScalablePushQueryMetadata;
import io.confluent.ksql.util.TransientQueryMetadata;
import io.vertx.core.Context;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("UnstableApiUsage")
final class PushQueryPublisher implements Flow.Publisher<Collection<StreamedRow>> {

  private static final Logger log = LoggerFactory.getLogger(PushQueryPublisher.class);

  private final ListeningScheduledExecutorService exec;
  private final PushQueryMetadata queryMetadata;

  private PushQueryPublisher(
      final ListeningScheduledExecutorService exec,
      final PushQueryMetadata queryMetadata
  ) {
    this.exec = exec;
    this.queryMetadata = queryMetadata;
  }

  public static PushQueryPublisher createPushQueryPublisher(
      final KsqlEngine ksqlEngine,
      final ServiceContext serviceContext,
      final ListeningScheduledExecutorService exec,
      final ConfiguredStatement<Query> query,
      final Optional<LocalCommands> localCommands
  ) {
    final TransientQueryMetadata queryMetadata =
        ksqlEngine.executeTransientQuery(serviceContext, query, true);

    localCommands.ifPresent(lc -> lc.write(queryMetadata));

    return new PushQueryPublisher(exec, queryMetadata);
  }

  public static PushQueryPublisher createScalablePushQueryPublisher(
      final KsqlEngine ksqlEngine,
      final ServiceContext serviceContext,
      final ListeningScheduledExecutorService exec,
      final ConfiguredStatement<Query> query,
      final ImmutableAnalysis analysis,
      final PushRouting pushRouting,
      final Context context,
      final  Optional<ScalablePushQueryMetrics> scalablePushQueryMetrics,
      final long startTimeNanos,
      final SlidingWindowRateLimiter scalablePushBandRateLimiter
  ) {
    final PushRoutingOptions routingOptions = new PushQueryConfigRoutingOptions(
        ImmutableMap.of()
    );

    final PushQueryConfigPlannerOptions plannerOptions = new PushQueryConfigPlannerOptions(
        query.getSessionConfig().getConfig(false),
        query.getSessionConfig().getOverrides());

    scalablePushBandRateLimiter.allow(KsqlQueryType.PUSH);

    ScalablePushQueryMetadata pushQueryMetadata = null;
    try {
      pushQueryMetadata = ksqlEngine
              .executeScalablePushQuery(analysis, serviceContext, query, pushRouting,
                      routingOptions, plannerOptions, context, scalablePushQueryMetrics);

      final ScalablePushQueryMetadata finalPushQueryMetadata = pushQueryMetadata;
      pushQueryMetadata.onCompletionOrException((v, throwable) ->
              scalablePushQueryMetrics.ifPresent(
                      m -> recordMetrics(m, finalPushQueryMetadata, startTimeNanos)));
      pushQueryMetadata.prepare();
    } catch (Throwable t) {
      if (pushQueryMetadata == null) {
        scalablePushQueryMetrics.ifPresent(m -> recordErrorMetrics(m, startTimeNanos));
      }
      throw t;
    }

    return new PushQueryPublisher(exec, pushQueryMetadata);
  }

  @Override
  public synchronized void subscribe(final Flow.Subscriber<Collection<StreamedRow>> subscriber) {

    final PushQuerySubscription subscription =
        new PushQuerySubscription(exec, subscriber, queryMetadata);

    log.info("Running query {}", queryMetadata.getQueryId().toString());
    queryMetadata.start();

    subscriber.onSubscribe(subscription);
  }


  private static void recordMetrics(
          final ScalablePushQueryMetrics metrics, final ScalablePushQueryMetadata metadata,
          final long startTimeNanos) {

    final QuerySourceType sourceType = metadata.getSourceType();
    final RoutingNodeType routingNodeType = metadata.getRoutingNodeType();
    // Note: we are not recording response size in this case because it is not
    // accessible in the websocket endpoint.
    metrics.recordConnectionDuration(startTimeNanos, sourceType, routingNodeType);
    metrics.recordRowsReturned(metadata.getTotalRowsReturned(),
        sourceType, routingNodeType);
    metrics.recordRowsProcessed(metadata.getTotalRowsProcessed(),
        sourceType, routingNodeType);
  }

  private static void recordErrorMetrics(
          final ScalablePushQueryMetrics metrics, final long startTimeNanos) {
    metrics.recordConnectionDurationForError(startTimeNanos);
    metrics.recordZeroRowsReturnedForError();
    metrics.recordZeroRowsProcessedForError();
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
              if (kv.getRowMetadata().isPresent()) {
                return StreamedRow.progressToken(new RowOffsets(
                    kv.getRowMetadata().get().getStartOffsets(),
                    kv.getRowMetadata().get().getOffsets()));
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
