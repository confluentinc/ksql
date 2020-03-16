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
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.confluent.ksql.rest.server.resources.streaming.Flow.Subscriber;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.TransientQueryMetadata;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.kafka.streams.KeyValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("UnstableApiUsage")
class PushQueryPublisher implements Flow.Publisher<Collection<StreamedRow>> {

  private static final Logger log = LoggerFactory.getLogger(PushQueryPublisher.class);

  private final KsqlEngine ksqlEngine;
  private final ServiceContext serviceContext;
  private final ConfiguredStatement<Query> query;
  private final ListeningScheduledExecutorService exec;

  PushQueryPublisher(
      final KsqlEngine ksqlEngine,
      final ServiceContext serviceContext,
      final ListeningScheduledExecutorService exec,
      final ConfiguredStatement<Query> query
  ) {
    this.ksqlEngine = requireNonNull(ksqlEngine, "ksqlEngine");
    this.serviceContext = requireNonNull(serviceContext, "serviceContext");
    this.exec = requireNonNull(exec, "exec");
    this.query = requireNonNull(query, "query");
  }

  @Override
  public synchronized void subscribe(final Flow.Subscriber<Collection<StreamedRow>> subscriber) {
    final TransientQueryMetadata queryMetadata = ksqlEngine.executeQuery(serviceContext, query);
    final PushQuerySubscription subscription = new PushQuerySubscription(subscriber, queryMetadata);

    log.info("Running query {}", queryMetadata.getQueryApplicationId());
    queryMetadata.start();

    subscriber.onSubscribe(subscription);
  }

  class PushQuerySubscription extends PollingSubscription<Collection<StreamedRow>> {

    private final TransientQueryMetadata queryMetadata;
    private boolean closed = false;

    PushQuerySubscription(
        final Subscriber<Collection<StreamedRow>> subscriber,
        final TransientQueryMetadata queryMetadata
    ) {
      super(exec, subscriber, queryMetadata.getLogicalSchema());
      this.queryMetadata = requireNonNull(queryMetadata, "queryMetadata");

      queryMetadata.setLimitHandler(this::setDone);
      queryMetadata.setUncaughtExceptionHandler(
          (thread, e) -> setError(e)
      );
    }

    @Override
    public Collection<StreamedRow> poll() {
      final List<KeyValue<String, GenericRow>> rows = Lists.newLinkedList();
      queryMetadata.getRowQueue().drainTo(rows);
      if (rows.isEmpty()) {
        return null;
      } else {
        return rows.stream().map(row -> StreamedRow.row(row.value))
            .collect(Collectors.toCollection(Lists::newLinkedList));
      }
    }

    @Override
    public synchronized void close() {
      if (!closed) {
        closed = true;
        log.info("Terminating query {}", queryMetadata.getQueryApplicationId());
        queryMetadata.close();
      }
    }
  }
}
