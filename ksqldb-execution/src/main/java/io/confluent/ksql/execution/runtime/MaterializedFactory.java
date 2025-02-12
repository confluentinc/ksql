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

package io.confluent.ksql.execution.runtime;

import io.confluent.ksql.GenericRow;
import java.time.Duration;
import java.util.Optional;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.processor.StateStore;

public class MaterializedFactory {

  public MaterializedFactory() {
  }

  private <K, S extends StateStore> Materialized<K, GenericRow, S> create(
      final Serde<K> keySerde,
      final Serde<GenericRow> valSerde,
      final Optional<String> name,
      final Optional<Duration> retention) {

    final Materialized<K, GenericRow, S> materialized;
    if (name.isPresent()) {
      materialized = Materialized
          .<K, GenericRow, S>as(name.get())
          .withKeySerde(keySerde)
          .withValueSerde(valSerde)
      ;
    } else {
      materialized = Materialized.with(keySerde, valSerde);
    }

    retention.ifPresent(materialized::withRetention);

    return materialized;
  }

  public <K, S extends StateStore> Materialized<K, GenericRow, S> create(
      final Serde<K> keySerde,
      final Serde<GenericRow> valSerde,
      final String name,
      final Duration retention) {
    return create(keySerde, valSerde, Optional.of(name), Optional.of(retention));
  }

  public <K, S extends StateStore> Materialized<K, GenericRow, S> create(
      final Serde<K> keySerde,
      final Serde<GenericRow> valSerde,
      final String name,
      final Optional<Duration> retention) {
    return create(keySerde, valSerde, Optional.of(name), retention);
  }

  public <K, S extends StateStore> Materialized<K, GenericRow, S> create(
      final Serde<K> keySerde,
      final Serde<GenericRow> valSerde,
      final String name) {
    return create(keySerde, valSerde, Optional.of(name), Optional.empty());
  }

  public <K, S extends StateStore> Materialized<K, GenericRow, S> create(
      final Serde<K> keySerde,
      final Serde<GenericRow> valSerde) {
    return create(keySerde, valSerde, Optional.empty(), Optional.empty());
  }
}
