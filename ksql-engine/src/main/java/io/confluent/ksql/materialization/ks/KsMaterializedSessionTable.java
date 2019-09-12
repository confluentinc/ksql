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

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.materialization.MaterializationException;
import io.confluent.ksql.materialization.MaterializedWindowedTable;
import io.confluent.ksql.materialization.Window;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlySessionStore;

/**
 * Kafka Streams impl of {@link MaterializedWindowedTable}.
 */
class KsMaterializedSessionTable implements MaterializedWindowedTable {

  private final KsStateStore stateStore;

  KsMaterializedSessionTable(final KsStateStore store) {
    this.stateStore = Objects.requireNonNull(store, "store");
  }

  @Override
  public Map<Window, GenericRow> get(
      final Struct key,
      final Instant lower,
      final Instant upper
  ) {
    try {
      final ReadOnlySessionStore<Struct, GenericRow> store = stateStore
          .store(QueryableStoreTypes.sessionStore());

      return findSession(store, key, lower, upper);
    } catch (final Exception e) {
      throw new MaterializationException("Failed to get value from materialized table", e);
    }
  }

  private static Map<Window, GenericRow> findSession(
      final ReadOnlySessionStore<Struct, GenericRow> store,
      final Struct key,
      final Instant lower,
      final Instant upper
  ) {
    try (KeyValueIterator<Windowed<Struct>, GenericRow> it = store.fetch(key)) {

      while (it.hasNext()) {
        final KeyValue<Windowed<Struct>, GenericRow> next = it.next();

        if (intersects(next.key.window().startTime(), lower, upper)) {

          final Window window = Window.of(
              next.key.window().startTime(),
              Optional.of(next.key.window().endTime())
          );

          return ImmutableMap.of(window, next.value);
        }
      }
    }

    return ImmutableMap.of();
  }

  private static boolean intersects(
      final Instant wndStart,
      final Instant lower,
      final Instant upper
  ) {
    return wndStart.equals(lower) // lower inclusive
        || wndStart.equals(upper) // upper inclusive
        || (lower.isBefore(wndStart) && wndStart.isBefore(upper));
  }
}
