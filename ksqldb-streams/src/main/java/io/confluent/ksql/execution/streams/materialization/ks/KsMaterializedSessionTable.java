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
import io.confluent.ksql.execution.streams.materialization.WindowedRow;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Window;
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
      final int partition,
      final Range<Instant> windowStart,
      final Range<Instant> windowEnd
  ) {
    try {
      final ReadOnlySessionStore<Struct, GenericRow> store = stateStore
          .store(QueryableStoreTypes.sessionStore(), partition);

      return findSession(store, key, windowStart, windowEnd);
    } catch (final Exception e) {
      throw new MaterializationException("Failed to get value from materialized table", e);
    }
  }

  private List<WindowedRow> findSession(
      final ReadOnlySessionStore<Struct, GenericRow> store,
      final Struct key,
      final Range<Instant> windowStart,
      final Range<Instant> windowEnd
  ) {
    try (KeyValueIterator<Windowed<Struct>, GenericRow> it = store.fetch(key)) {

      final Builder<WindowedRow> builder = ImmutableList.builder();

      while (it.hasNext()) {
        final KeyValue<Windowed<Struct>, GenericRow> next = it.next();
        final Window wnd = next.key.window();

        if (!windowStart.contains(wnd.startTime())) {
          continue;
        }

        if (!windowEnd.contains(wnd.endTime())) {
          continue;
        }

        final long rowTime = wnd.end();

        final WindowedRow row = WindowedRow.of(
            stateStore.schema(),
            next.key,
            next.value,
            rowTime
        );

        builder.add(row);
      }

      return builder.build();
    }
  }
}
