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

import io.confluent.ksql.util.KsqlConfig;
import java.util.Map;
import java.util.Objects;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.Grouped;

public class KsqlGroupedFactory implements GroupedFactory {
  private final KsqlConfig ksqlConfig;
  private final StreamsStatics streamsStatics;

  public KsqlGroupedFactory(final KsqlConfig ksqlConfig) {
    this(ksqlConfig, new RealStreamsStatics());
  }

  public KsqlGroupedFactory(
      final KsqlConfig ksqlConfig,
      final StreamsStatics streamsStatics) {
    this.ksqlConfig = Objects.requireNonNull(ksqlConfig);
    this.streamsStatics = Objects.requireNonNull(streamsStatics);
  }

  @Override
  public <K, V> Grouped<K, V> create(
      final String name,
      final Serde<K> keySerde,
      final Serde<V> valSerde) {
    final Map<String, Object> streamsConfigs = ksqlConfig.getKsqlStreamConfigProps();
    final String groupedName =
        Objects.equals(streamsConfigs.get(TOPOLOGY_OPTIMIZATION), OPTIMIZE) ? name : null;
    return streamsStatics.groupedWith(groupedName, keySerde, valSerde);
  }
}
