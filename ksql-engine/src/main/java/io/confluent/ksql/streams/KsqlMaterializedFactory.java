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

package io.confluent.ksql.streams;

import static org.apache.kafka.streams.StreamsConfig.OPTIMIZE;
import static org.apache.kafka.streams.StreamsConfig.TOPOLOGY_OPTIMIZATION;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Map;
import java.util.Objects;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.processor.StateStore;

public class KsqlMaterializedFactory implements MaterializedFactory {
  final KsqlConfig ksqlConfig;
  final StreamsStatics streamsStatics;

  public KsqlMaterializedFactory(
      final KsqlConfig ksqlConfig,
      final StreamsStatics streamsStatics) {
    this.ksqlConfig = ksqlConfig;
    this.streamsStatics = streamsStatics;
  }

  public KsqlMaterializedFactory(final KsqlConfig ksqlConfig) {
    this(ksqlConfig, new RealStreamsStatics());
  }

  @Override
  public <K, S extends StateStore> Materialized<K, GenericRow, S> create(
      final Serde<K> keySerde,
      final Serde<GenericRow> valueSerde,
      final String opName) {
    final Map<String, Object> streamsConfigs = ksqlConfig.getKsqlStreamConfigProps();
    if (Objects.equals(streamsConfigs.get(TOPOLOGY_OPTIMIZATION), OPTIMIZE)) {
      return streamsStatics.<K, GenericRow, S>materializedAs(opName)
          .withKeySerde(keySerde)
          .withValueSerde(valueSerde);
    } else {
      return streamsStatics.materializedWith(keySerde, valueSerde);
    }
  }
}
