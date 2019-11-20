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
import io.confluent.ksql.execution.streams.materialization.MaterializationTimeOutException;
import io.confluent.ksql.execution.streams.materialization.NotRunningException;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.util.KsqlConfig;
import java.util.function.Supplier;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.state.QueryableStoreType;

/**
 * Wrapper around Kafka Streams state store.
 */
class KsStateStore {

  private final String stateStoreName;
  private final KafkaStreams kafkaStreams;
  private final LogicalSchema schema;
  private final KsqlConfig ksqlConfig;
  private final Supplier<Long> clock;

  KsStateStore(
      final String stateStoreName,
      final KafkaStreams kafkaStreams,
      final LogicalSchema schema,
      final KsqlConfig ksqlConfig
  ) {
    this(stateStoreName, kafkaStreams, schema, ksqlConfig, System::currentTimeMillis);
  }

  @VisibleForTesting
  KsStateStore(
      final String stateStoreName,
      final KafkaStreams kafkaStreams,
      final LogicalSchema schema,
      final KsqlConfig ksqlConfig,
      final Supplier<Long> clock
  ) {
    this.kafkaStreams = requireNonNull(kafkaStreams, "kafkaStreams");
    this.stateStoreName = requireNonNull(stateStoreName, "stateStoreName");
    this.schema = requireNonNull(schema, "schema");
    this.ksqlConfig = requireNonNull(ksqlConfig, "ksqlConfig");
    this.clock = requireNonNull(clock, "clock");

    if (!schema.metadata().isEmpty()) {
      throw new IllegalArgumentException("Kafka Streams state stores do no expose meta columns");
    }
  }

  LogicalSchema schema() {
    return schema;
  }

  <T> T store(final QueryableStoreType<T> queryableStoreType) {
    awaitRunning();

    try {
      return kafkaStreams.store(stateStoreName, queryableStoreType);
    } catch (final Exception e) {
      final State state = kafkaStreams.state();
      if (state != State.RUNNING) {
        throw new NotRunningException("The query was not in a running state. state: " + state);
      }

      throw new MaterializationException("State store currently unavailable: " + stateStoreName, e);
    }
  }

  private void awaitRunning() {
    final long timeoutMs =
        ksqlConfig.getLong(KsqlConfig.KSQL_QUERY_PULL_STREAMSTORE_REBALANCING_TIMEOUT_MS_CONFIG);
    final long threshold = clock.get() + timeoutMs;
    while (kafkaStreams.state() == State.REBALANCING) {
      if (clock.get() > threshold) {
        throw new MaterializationTimeOutException("Store failed to rebalance within the configured "
            + "timeout. timeout: " + timeoutMs + "ms, config: "
            + KsqlConfig.KSQL_QUERY_PULL_STREAMSTORE_REBALANCING_TIMEOUT_MS_CONFIG);
      }

      Thread.yield();
    }
  }
}
