/**
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.rest.server.resources.streaming;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.KsqlEngine;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.confluent.ksql.rest.server.resources.streaming.Flow.Subscriber;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.QueuedQueryMetadata;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.kafka.streams.KeyValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamPublisher implements Flow.Publisher<Collection<StreamedRow>> {

  private static final Logger log = LoggerFactory.getLogger(StreamPublisher.class);

  private final KsqlConfig ksqlConfig;
  private final KsqlEngine ksqlEngine;
  private final String queryString;
  private final Map<String, Object> clientLocalProperties;
  private final ListeningScheduledExecutorService exec;

  public StreamPublisher(
      final KsqlConfig ksqlConfig,
      final KsqlEngine ksqlEngine,
      final ListeningScheduledExecutorService exec,
      final String queryString,
      final Map<String, Object> clientLocalProperties
  ) {
    this.ksqlConfig = ksqlConfig;
    this.ksqlEngine = ksqlEngine;
    this.exec = exec;
    this.queryString = queryString;
    this.clientLocalProperties = clientLocalProperties;
  }

  @Override
  public synchronized void subscribe(final Flow.Subscriber<Collection<StreamedRow>> subscriber) {
    final QueuedQueryMetadata queryMetadata = (QueuedQueryMetadata) ksqlEngine.buildMultipleQueries(
        queryString,
        ksqlConfig,
        clientLocalProperties).get(0);

    final StreamSubscription subscription = new StreamSubscription(subscriber, queryMetadata);

    log.info("Running query {}", queryMetadata.getQueryApplicationId());
    queryMetadata.getKafkaStreams().start();

    subscriber.onSubscribe(subscription);
  }

  class StreamSubscription extends PollingSubscription<Collection<StreamedRow>> {

    private final QueuedQueryMetadata queryMetadata;
    private boolean closed = false;

    StreamSubscription(
        final Subscriber<Collection<StreamedRow>> subscriber,
        final QueuedQueryMetadata queryMetadata
    ) {
      super(exec, subscriber, queryMetadata.getResultSchema());
      this.queryMetadata = queryMetadata;

      queryMetadata.setLimitHandler(this::setDone);
      queryMetadata.getKafkaStreams().setUncaughtExceptionHandler(
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
        ksqlEngine.removeTemporaryQuery(queryMetadata);
      }
    }
  }
}
