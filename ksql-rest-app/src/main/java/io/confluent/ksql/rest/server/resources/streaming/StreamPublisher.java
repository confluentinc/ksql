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

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.streams.KeyValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.KsqlEngine;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.confluent.ksql.rest.server.resources.streaming.PollingSubscription.Pollable;
import io.confluent.ksql.util.QueuedQueryMetadata;

public class StreamPublisher implements Flow.Publisher<Collection<StreamedRow>> {

  private static final Logger log = LoggerFactory.getLogger(StreamPublisher.class);

  private final KsqlEngine ksqlEngine;
  private final String queryString;
  private final Map<String, Object> clientLocalProperties;
  private final ListeningScheduledExecutorService exec;

  private boolean closed = false;
  private QueuedQueryMetadata queryMetadata;
  private volatile Throwable error;

  public StreamPublisher(
      KsqlEngine ksqlEngine,
      ListeningScheduledExecutorService exec,
      String queryString,
      Map<String, Object> clientLocalProperties
  ) {
    this.ksqlEngine = ksqlEngine;
    this.exec = exec;
    this.queryString = queryString;
    this.clientLocalProperties = clientLocalProperties;
  }

  @Override
  public synchronized void subscribe(Flow.Subscriber<Collection<StreamedRow>> subscriber) {
    if (queryMetadata != null) {
      throw new IllegalStateException("already subscribed");
    }

    queryMetadata = (QueuedQueryMetadata) ksqlEngine.buildMultipleQueries(
        queryString,
        clientLocalProperties
    ).get(0);

    queryMetadata.getKafkaStreams().setUncaughtExceptionHandler(
        (thread, e) -> error = e
    );
    log.info("Running query {}", queryMetadata.getQueryApplicationId());
    queryMetadata.getKafkaStreams().start();

    subscriber.onSubscribe(
        new PollingSubscription<>(exec, subscriber, new Pollable<Collection<StreamedRow>>() {
          @Override
          public Schema getSchema() {
            return queryMetadata.getResultSchema();
          }

          @Override
          public Collection<StreamedRow> poll() {
            List<KeyValue<String, GenericRow>> rows = Lists.newLinkedList();
            queryMetadata.getRowQueue().drainTo(rows);
            if (rows.isEmpty()) {
              return null;
            } else {
              return Lists.transform(rows, row -> new StreamedRow(row.value));
            }
          }

          @Override
          public boolean hasError() {
            return error != null;
          }

          @Override
          public Throwable getError() {
            return error;
          }

          @Override
          public void close() {
            StreamPublisher.this.close();
          }
        })
    );
  }

  private synchronized void close() {
    if (!closed) {
      closed = true;
      log.info("Terminating query {}", queryMetadata.getQueryApplicationId());
      queryMetadata.close();
      ksqlEngine.removeTemporaryQuery(queryMetadata);
    }
  }
}
