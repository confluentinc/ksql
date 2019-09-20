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

package io.confluent.ksql.materialization.ks;

import static java.util.Objects.requireNonNull;

import com.google.common.annotations.VisibleForTesting;
import io.confluent.ksql.materialization.MaterializationException;
import io.confluent.ksql.materialization.MaterializationTimeOutException;
import io.confluent.ksql.materialization.NotRunningException;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.support.metrics.common.time.Clock;
import java.time.Duration;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.state.QueryableStoreType;

/**
 * Wrapper around Kafka Streams state store.
 */
class KsStateStore {

  private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(10);

  private final String stateStoreName;
  private final KafkaStreams kafkaStreams;
  private final LogicalSchema schema;
  private final Duration timeout;
  private final Clock clock;

  KsStateStore(
      final String stateStoreName,
      final KafkaStreams kafkaStreams,
      final LogicalSchema schema
  ) {
    this(stateStoreName, kafkaStreams, schema, DEFAULT_TIMEOUT, System::currentTimeMillis);
  }

  @VisibleForTesting
  KsStateStore(
      final String stateStoreName,
      final KafkaStreams kafkaStreams,
      final LogicalSchema schema,
      final Duration timeout,
      final Clock clock
  ) {
    this.kafkaStreams = requireNonNull(kafkaStreams, "kafkaStreams");
    this.stateStoreName = requireNonNull(stateStoreName, "stateStoreName");
    this.schema = requireNonNull(schema, "schema");
    this.timeout = requireNonNull(timeout, "timeout");
    this.clock = requireNonNull(clock, "clock");
  }

  LogicalSchema schema() {
    return schema;
  }

  <T> T store(final QueryableStoreType<T> queryableStoreType) {
    awaitRunning();

    try {
      return kafkaStreams.store(stateStoreName, queryableStoreType);
    } catch (final Exception e) {
      throw new MaterializationException("State store currently unavailable: " + stateStoreName, e);
    }
  }

  private void awaitRunning() {
    final long threshold = clock.currentTimeMs() + timeout.toMillis();
    while (kafkaStreams.state() == State.REBALANCING) {
      if (clock.currentTimeMs() > threshold) {
        throw new MaterializationTimeOutException("Store failed to rebalance within the configured "
            + "timeout. timeout: " + timeout.toMillis() + "ms");
      }

      Thread.yield();
    }

    final State state = kafkaStreams.state();
    if (state != State.RUNNING) {
      throw new NotRunningException("The query was not in a running state. state: " + state);
    }
  }
}
