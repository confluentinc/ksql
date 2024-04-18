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

package io.confluent.ksql.execution.streams.materialization.ks;

import static java.util.Objects.requireNonNull;

import com.google.common.annotations.VisibleForTesting;
import io.confluent.ksql.execution.streams.materialization.MaterializationException;
import io.confluent.ksql.execution.streams.materialization.NotRunningException;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.util.KsqlConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.processor.internals.namedtopology.KafkaStreamsNamedTopologyWrapper;
import org.apache.kafka.streams.processor.internals.namedtopology.NamedTopologyStoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreType;

/**
 * Wrapper around Kafka Streams state store.
 */
class KsStateStore {

  private final String stateStoreName;
  private final KafkaStreams kafkaStreams;
  private final LogicalSchema schema;
  private final KsqlConfig ksqlConfig;
  private final String queryId;

  @VisibleForTesting
  KsStateStore(
      final String stateStoreName,
      final KafkaStreams kafkaStreams,
      final LogicalSchema schema,
      final KsqlConfig ksqlConfig,
      final String queryId
  ) {
    this.kafkaStreams = requireNonNull(kafkaStreams, "kafkaStreams");
    this.stateStoreName = requireNonNull(stateStoreName, "stateStoreName");
    this.schema = requireNonNull(schema, "schema");
    this.ksqlConfig = requireNonNull(ksqlConfig, "ksqlConfig");
    this.queryId = requireNonNull(queryId, "queryId");
  }

  LogicalSchema schema() {
    return schema;
  }

  String getStateStoreName() {
    return stateStoreName;
  }

  KafkaStreams getKafkaStreams() {
    return kafkaStreams;
  }

  KsqlConfig getKsqlConfig() {
    return ksqlConfig;
  }

  <T> T store(final QueryableStoreType<T> queryableStoreType, final int partition) {
    try {
      final boolean enableStaleStores =
          ksqlConfig.getBoolean(KsqlConfig.KSQL_QUERY_PULL_ENABLE_STANDBY_READS);
      final boolean sharedRuntime = kafkaStreams instanceof KafkaStreamsNamedTopologyWrapper;

      final StoreQueryParameters<T> parameters = sharedRuntime
          ? NamedTopologyStoreQueryParameters.fromNamedTopologyAndStoreNameAndType(
              queryId,
              stateStoreName,
              queryableStoreType).withPartition(partition)
          : StoreQueryParameters.fromNameAndType(
              stateStoreName,
              queryableStoreType).withPartition(partition);

      return enableStaleStores
          ? kafkaStreams.store(parameters.enableStaleStores())
          : kafkaStreams.store(parameters);
    } catch (final Exception e) {
      final State state = kafkaStreams.state();
      if (state != State.RUNNING) {
        throw new NotRunningException("The query was not in a running state. state: " + state);
      }

      throw new MaterializationException("State store currently unavailable: " + stateStoreName, e);
    }
  }
}
