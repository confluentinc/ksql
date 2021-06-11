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
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.physical.scalablepush.PushRouting;
import io.confluent.ksql.physical.scalablepush.PushRoutingOptions;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.confluent.ksql.rest.server.LocalCommands;
import io.confluent.ksql.rest.server.resources.streaming.Flow.Subscriber;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.LogicalSchema.Builder;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KeyValue;
import io.confluent.ksql.util.PushQueryMetadata;
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

  private final KsqlEngine ksqlEngine;
  private final ServiceContext serviceContext;
  private final ConfiguredStatement<Query> query;
  private final Optional<LocalCommands> localCommands;
  private final ListeningScheduledExecutorService exec;
  private final PushRouting pushRouting;
  private final boolean isScalablePush;
  private final Context context;

  private PushQueryPublisher(
      final KsqlEngine ksqlEngine,
      final ServiceContext serviceContext,
      final ListeningScheduledExecutorService exec,
      final ConfiguredStatement<Query> query,
      final Optional<LocalCommands> localCommands
  ) {
    this.ksqlEngine = requireNonNull(ksqlEngine, "ksqlEngine");
    this.serviceContext = requireNonNull(serviceContext, "serviceContext");
    this.exec = requireNonNull(exec, "exec");
    this.query = requireNonNull(query, "query");
    this.localCommands = requireNonNull(localCommands, "localCommands");
    this.pushRouting = null;
    this.isScalablePush = false;
    this.context = null;
  }

  private PushQueryPublisher(
      final KsqlEngine ksqlEngine,
      final ServiceContext serviceContext,
      final ListeningScheduledExecutorService exec,
      final ConfiguredStatement<Query> query,
      final PushRouting pushRouting,
      final Context context
  ) {
    this.ksqlEngine = requireNonNull(ksqlEngine, "ksqlEngine");
    this.serviceContext = requireNonNull(serviceContext, "serviceContext");
    this.exec = requireNonNull(exec, "exec");
    this.query = requireNonNull(query, "query");
    this.localCommands = Optional.empty();
    this.pushRouting = requireNonNull(pushRouting, "pushRouting");
    this.isScalablePush = true;
    this.context = requireNonNull(context, "context");
  }

  public static PushQueryPublisher createPublisher(
      final KsqlEngine ksqlEngine,
      final ServiceContext serviceContext,
      final ListeningScheduledExecutorService exec,
      final ConfiguredStatement<Query> query,
      final Optional<LocalCommands> localCommands
  ) {
    return new PushQueryPublisher(ksqlEngine, serviceContext, exec, query, localCommands);
  }

  public static PushQueryPublisher createScalablePublisher(
      final KsqlEngine ksqlEngine,
      final ServiceContext serviceContext,
      final ListeningScheduledExecutorService exec,
      final ConfiguredStatement<Query> query,
      final PushRouting pushRouting,
      final Context context
  ) {
    return new PushQueryPublisher(ksqlEngine, serviceContext, exec, query, pushRouting, context);
  }

  @Override
  public synchronized void subscribe(final Flow.Subscriber<Collection<StreamedRow>> subscriber) {
    final PushQueryMetadata queryMetadata;
    if (isScalablePush) {
      final PushRoutingOptions routingOptions = new PushQueryConfigRoutingOptions(
          ImmutableMap.of()
      );

      final PushQueryConfigPlannerOptions plannerOptions = new PushQueryConfigPlannerOptions(
              query.getSessionConfig().getConfig(false),
              query.getSessionConfig().getOverrides());

      queryMetadata = ksqlEngine
          .executeScalablePushQuery(serviceContext, query, pushRouting, routingOptions,
              plannerOptions, context);
    } else {
      queryMetadata = ksqlEngine
          .executeQuery(serviceContext, query, true);

      localCommands.ifPresent(lc -> lc.write((TransientQueryMetadata) queryMetadata));
    }

    final PushQuerySubscription subscription =
        new PushQuerySubscription(exec, subscriber, queryMetadata);

    log.info("Running query {}", queryMetadata.getQueryId().toString());
    queryMetadata.start();

    subscriber.onSubscribe(subscription);
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
      queryMetadata.setUncaughtExceptionHandler(
          e -> {
            setError(e);
            return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
          }
      );
    }

    @Override
    public Collection<StreamedRow> poll() {
      final List<KeyValue<List<?>, GenericRow>> rows = Lists.newLinkedList();
      queryMetadata.getRowQueue().drainTo(rows);
      if (rows.isEmpty()) {
        return null;
      } else {
        return rows.stream()
            .map(kv -> StreamedRow.pushRow(kv.value()))
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
