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

package io.confluent.ksql.execution.streams;

import io.confluent.ksql.GenericRow;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.processor.StateStore;

public interface MaterializedFactory {
  <K, S extends StateStore> Materialized<K, GenericRow, S> create(
      Serde<K> keySerde, Serde<GenericRow> valSerde, String name);

  static MaterializedFactory create() {
    return create(
        new Materializer() {
          @Override
          public <K, V, S extends StateStore> Materialized<K, V, S> materializedWith(
              final Serde<K> keySerde,
              final Serde<V> valueSerde) {
            return Materialized.with(keySerde, valueSerde);
          }

          @Override
          public <K, V, S extends StateStore> Materialized<K, V, S> materializedAs(
              final String storeName) {
            return Materialized.as(storeName);
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
          final String name) {
        return materializer.<K, GenericRow, S>materializedAs(name)
            .withKeySerde(keySerde)
            .withValueSerde(valSerde);
      }
    };
  }

  interface Materializer {
    <K, V, S extends StateStore> Materialized<K, V, S> materializedWith(
        Serde<K> keySerde,
        Serde<V> valueSerde);

    <K, V, S extends StateStore> Materialized<K, V, S> materializedAs(String storeName);
  }
}
