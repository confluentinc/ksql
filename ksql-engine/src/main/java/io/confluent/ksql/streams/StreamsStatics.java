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

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.processor.StateStore;

public interface StreamsStatics {
  <K, V> Grouped<K, V> groupedWith(String name, Serde<K> keySerde, Serde<V> valSerde);

  <K, V, V0> Joined<K, V, V0> joinedWith(
      Serde<K> keySerde,
      Serde<V> leftSerde,
      Serde<V0> rightSerde,
      String name);

  <K, V, S extends StateStore> Materialized<K, V, S> materializedWith(
      Serde<K> keySerde,
      Serde<V> valueSerde);

  <K, V, S extends StateStore> Materialized<K, V, S> materializedAs(String storeName);
}
