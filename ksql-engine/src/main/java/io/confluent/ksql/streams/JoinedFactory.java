/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.streams;

import io.confluent.ksql.util.KsqlConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.Joined;

public interface JoinedFactory {
  <K, V, V0> Joined<K, V, V0> create(
      Serde<K> keySerde,
      Serde<V> leftSerde,
      Serde<V0> rightSerde,
      String name);

  static JoinedFactory create(final KsqlConfig ksqlConfig) {
    return create(ksqlConfig, Joined::with);
  }

  static JoinedFactory create(final KsqlConfig ksqlConfig, final Joiner joiner) {
    if (StreamsUtil.useProvidedName(ksqlConfig)) {
      return joiner::joinedWith;
    }
    return new JoinedFactory() {
      @Override
      public <K, V, V0> Joined<K, V, V0> create(
          final Serde<K> keySerde,
          final Serde<V> leftSerde,
          final Serde<V0> rightSerde,
          final String name) {
        return joiner.joinedWith(keySerde, leftSerde, rightSerde, null);
      }
    };
  }

  @FunctionalInterface
  interface Joiner {
    <K, V, V0> Joined<K, V, V0> joinedWith(
        Serde<K> keySerde,
        Serde<V> leftSerde,
        Serde<V0> rightSerde,
        String name);
  }
}
