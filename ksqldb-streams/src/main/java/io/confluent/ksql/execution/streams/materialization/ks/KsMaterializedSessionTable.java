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
import io.confluent.ksql.GenericKey;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.streams.materialization.MaterializationException;
import io.confluent.ksql.execution.streams.materialization.MaterializedWindowedTable;
import io.confluent.ksql.execution.streams.materialization.StreamsMaterializedWindowedTable;
import io.confluent.ksql.execution.streams.materialization.WindowedRow;
import io.confluent.ksql.execution.streams.materialization.ks.SessionStoreCacheBypass.SessionStoreCacheBypassFetcher;
import io.confluent.ksql.execution.streams.materialization.ks.SessionStoreCacheBypass.SessionStoreCacheBypassFetcherRange;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Window;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlySessionStore;

/**
 * Kafka Streams impl of {@link MaterializedWindowedTable}.
 */
class KsMaterializedSessionTable implements StreamsMaterializedWindowedTable {

  private final KsStateStore stateStore;
  private final SessionStoreCacheBypassFetcher cacheBypassFetcher;
  private final SessionStoreCacheBypassFetcherRange cacheBypassFetcherRange;

  KsMaterializedSessionTable(final KsStateStore store,
                             final SessionStoreCacheBypassFetcher cacheBypassFetcher,
                             final SessionStoreCacheBypassFetcherRange cacheBypassFetcherRange) {
    this.stateStore = Objects.requireNonNull(store, "store");
    this.cacheBypassFetcher = Objects.requireNonNull(cacheBypassFetcher, "cacheBypassFetcher");
    this.cacheBypassFetcherRange = Objects.requireNonNull(cacheBypassFetcherRange,
            "cacheBypassFetcherRange");
  }

  @Override
  public KsMaterializedQueryResult<WindowedRow> get(
      final GenericKey key,
      final int partition,
      final Range<Instant> windowStart,
      final Range<Instant> windowEnd,
      final Optional<Position> position
  ) {
    try {
      final ReadOnlySessionStore<GenericKey, GenericRow> store = stateStore
          .store(QueryableStoreTypes.sessionStore(), partition);

      return KsMaterializedQueryResult.rowIterator(
          findSession(store, key, windowStart, windowEnd).iterator());
    } catch (final Exception e) {
      throw new MaterializationException("Failed to get value from materialized table", e);
    }
  }

  @Override
  public KsMaterializedQueryResult<WindowedRow> get(
      final int partition,
      final Range<Instant> windowStartBounds,
      final Range<Instant> windowEndBounds,
      final Optional<Position> position
  ) {
    throw new MaterializationException("Table scan unsupported on session tables");
  }

  private List<WindowedRow> findSession(
      final ReadOnlySessionStore<GenericKey, GenericRow> store,
      final GenericKey key,
      final Range<Instant> windowStart,
      final Range<Instant> windowEnd
  ) {
    try (KeyValueIterator<Windowed<GenericKey>, GenericRow> it =
        cacheBypassFetcher.fetch(store, key)) {

      final Builder<WindowedRow> builder = ImmutableList.builder();

      while (it.hasNext()) {
        final KeyValue<Windowed<GenericKey>, GenericRow> next = it.next();
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
