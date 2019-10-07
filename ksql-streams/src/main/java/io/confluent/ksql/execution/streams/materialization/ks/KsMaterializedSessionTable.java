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

package io.confluent.ksql.execution.streams.materialization.ks;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.Range;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.streams.materialization.MaterializationException;
import io.confluent.ksql.execution.streams.materialization.MaterializedWindowedTable;
import io.confluent.ksql.execution.streams.materialization.Window;
import io.confluent.ksql.execution.streams.materialization.WindowedRow;
import java.time.Instant;
import java.util.List;
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
  public List<WindowedRow> get(
      final Struct key,
      final Range<Instant> windowStart
  ) {
    try {
      final ReadOnlySessionStore<Struct, GenericRow> store = stateStore
          .store(QueryableStoreTypes.sessionStore());

      return findSession(store, key, windowStart);
    } catch (final Exception e) {
      throw new MaterializationException("Failed to get value from materialized table", e);
    }
  }

  private List<WindowedRow> findSession(
      final ReadOnlySessionStore<Struct, GenericRow> store,
      final Struct key,
      final Range<Instant> windowStart
  ) {
    try (KeyValueIterator<Windowed<Struct>, GenericRow> it = store.fetch(key)) {

      final Builder<WindowedRow> builder = ImmutableList.builder();

      while (it.hasNext()) {
        final KeyValue<Windowed<Struct>, GenericRow> next = it.next();

        if (windowStart.contains(next.key.window().startTime())) {

          final Window window = Window.of(
              next.key.window().startTime(),
              Optional.of(next.key.window().endTime())
          );

          builder.add(WindowedRow.of(stateStore.schema(), key, window, next.value));
        }
      }

      return builder.build();
    }
  }
}
