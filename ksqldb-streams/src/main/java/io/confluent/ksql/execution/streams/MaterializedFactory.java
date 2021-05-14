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

package io.confluent.ksql.execution.streams;

import io.confluent.ksql.GenericRow;
import java.time.Duration;
import java.util.Optional;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.processor.StateStore;

public interface MaterializedFactory {
  <K, S extends StateStore> Materialized<K, GenericRow, S> create(
      Serde<K> keySerde,
      Serde<GenericRow> valSerde,
      String name,
      Optional<Duration> retention
  );

  <K, S extends StateStore> Materialized<K, GenericRow, S> create(
      Serde<K> keySerde,
      Serde<GenericRow> valSerde,
      String name
  );

  static MaterializedFactory create() {
    return create(
        new Materializer() {
          @Override
          @SuppressWarnings("unchecked")
          public <K, V, S extends StateStore> Materialized<K, V, S> materializedAs(
              final String storeName,
              final Optional<Duration> retention) {
            if (retention.isPresent()) {
              return (Materialized<K, V, S>) Materialized.as(storeName)
                  .withRetention(retention.get());
            } else {
              return Materialized.as(storeName);
            }
          }
        }
    );
  }

  static MaterializedFactory create(final Materializer materializer) {
    return new MaterializedFactory() {
      @Override
      public <K, S extends StateStore> Materialized<K, GenericRow, S> create(
          final Serde<K> keySerde,
          final Serde<GenericRow> valSerde,
          final String name,
          final Optional<Duration> retention) {
        return materializer.<K, GenericRow, S>materializedAs(name, retention)
            .withKeySerde(keySerde)
            .withValueSerde(valSerde);
      }

      @Override
      public <K, S extends StateStore> Materialized<K, GenericRow, S> create(
          final Serde<K> keySerde,
          final Serde<GenericRow> valSerde,
          final String name) {
        return create(keySerde, valSerde, name, Optional.empty());
      }
    };
  }

  interface Materializer {
    <K, V, S extends StateStore> Materialized<K, V, S> materializedAs(
        String storeName,
        Optional<Duration> retention);
  }
}
