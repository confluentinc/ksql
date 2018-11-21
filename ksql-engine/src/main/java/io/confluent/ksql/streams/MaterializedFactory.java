/*
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

package io.confluent.ksql.streams;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.util.KsqlConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.processor.StateStore;

public interface MaterializedFactory {
  <K, S extends StateStore> Materialized<K, GenericRow, S> create(
      Serde<K> keySerde, Serde<GenericRow> valSerde, String name);

  static MaterializedFactory create(final KsqlConfig ksqlConfig) {
    return create(ksqlConfig, new RealStreamsStatics());
  }

  static MaterializedFactory create(
      final KsqlConfig ksqlConfig,
      final StreamsStatics streamsStatics) {
    if (StreamsUtil.useProvidedName(ksqlConfig)) {
      return new MaterializedFactory() {
        @Override
        public <K, S extends StateStore> Materialized<K, GenericRow, S> create(
            final Serde<K> keySerde,
            final Serde<GenericRow> valSerde,
            final String name) {
          return streamsStatics.<K, GenericRow, S>materializedAs(name)
              .withKeySerde(keySerde)
              .withValueSerde(valSerde);
        }
      };
    }
    return new MaterializedFactory() {
      @Override
      public <K, S extends StateStore> Materialized<K, GenericRow, S> create(
          final Serde<K> keySerde,
          final Serde<GenericRow> valSerde,
          final String name) {
        return streamsStatics.materializedWith(keySerde, valSerde);
      }
    };
  }
}
