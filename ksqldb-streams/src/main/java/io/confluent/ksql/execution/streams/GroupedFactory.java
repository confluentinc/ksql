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

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.Grouped;

public interface GroupedFactory {
  <K, V> Grouped<K, V> create(String name, Serde<K> keySerde, Serde<V> valSerde);

  static GroupedFactory create() {
    return create(Grouped::with);
  }

  static GroupedFactory create(final Grouper grouper) {
    return grouper::groupedWith;
  }

  @FunctionalInterface
  interface Grouper {
    <K, V> Grouped<K, V> groupedWith(String name, Serde<K> keySerde, Serde<V> valSerde);
  }
}
