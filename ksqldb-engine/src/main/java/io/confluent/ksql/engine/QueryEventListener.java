/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql.engine;

import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.query.QueryError;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.QueryMetadata;
import java.util.Optional;
import org.apache.kafka.streams.KafkaStreams;

/**
 * Defines the events that clients of {@link io.confluent.ksql.query.QueryRegistry} can register
 * to listen on.
 */
public interface QueryEventListener {

  /**
   * Called when a new query is created.
   * @param serviceContext service context
   * @param metaStore meta store
   * @param queryMetadata handle for the query that was just created
   */
  default void onCreate(
      ServiceContext serviceContext,
      MetaStore metaStore,
      QueryMetadata queryMetadata
  ) {
  }

  /**
   * Called when the state of the underlying Kafka Streams application of a given query changes
   * @param query the query whose state has changed
   * @param before the Kafka Streams state before the state change
   * @param after the Kafka Streams state after the state change
   */
  default void onStateChange(
      QueryMetadata query,
      KafkaStreams.State before,
      KafkaStreams.State after
  ) {
  }

  /**
   * Called when the ksqlDB query is paused or resumed.
   * @param query the query whose state has changed
   */
  default void onKsqlStateChange(
          QueryMetadata query
  ) {
  }

  /**
   * Called when a query hits a query execution error
   * @param query the query that hit the error
   * @param error the error that the query hit
   */
  default void onError(QueryMetadata query, QueryError error) {
  }

  /**
   * Called when a query is removed from the registry. This happens either because the query was
   * closed, in which case the query is stopped and its resources are destroyed, or the registry
   * was closed, in which case the query is stopped and its resources _may_ be destroyed, depending
   * on the flag passed to {@link io.confluent.ksql.query.QueryRegistry}.close.
   *
   * @param query the query that was deregistered
   */
  default void onDeregister(QueryMetadata query) {
  }

  /**
   * Called when a query is closed, meaning the query is stopped, and all its resources are
   * destroyed.
   * @param query the query that was closed
   */
  default void onClose(QueryMetadata query) {
  }

  /**
   * Called when the query registry is creating a validation sandbox. If the event listener
   * implementation wants to listen for events on the sandbox registry, it should return a
   * QueryEventListener implementation when this method is called. Otherwise, return
   * {@code Optional.empty()}
   *
   * @return the sandbox event listener
   */
  default Optional<QueryEventListener> createSandbox() {
    return Optional.empty();
  }
}
