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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.Range;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.materialization.MaterializationException;
import io.confluent.ksql.materialization.MaterializedWindowedTable;
import io.confluent.ksql.materialization.Window;
import io.confluent.ksql.materialization.WindowedRow;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;

/**
 * Kafka Streams impl of {@link MaterializedWindowedTable}.
 */
class KsMaterializedWindowTable implements MaterializedWindowedTable {

  private final KsStateStore stateStore;

  KsMaterializedWindowTable(final KsStateStore store) {
    this.stateStore = Objects.requireNonNull(store, "store");
  }

  @Override
  public List<WindowedRow> get(
      final Struct key,
      final Range<Instant> windowStart
  ) {
    try {
      final ReadOnlyWindowStore<Struct, GenericRow> store = stateStore
          .store(QueryableStoreTypes.windowStore());

      final Instant lower = windowStart.lowerEndpoint();
      final Instant upper = windowStart.upperEndpoint();

      try (WindowStoreIterator<GenericRow> it = store.fetch(key, lower, upper)) {

        final Builder<WindowedRow> builder = ImmutableList.builder();

        while (it.hasNext()) {
          final KeyValue<Long, GenericRow> next = it.next();
          final Window window = Window.of(Instant.ofEpochMilli(next.key), Optional.empty());
          builder.add(WindowedRow.of(stateStore.schema(), key, window, next.value));
        }

        return builder.build();
      }
    } catch (final Exception e) {
      throw new MaterializationException("Failed to get value from materialized table", e);
    }
  }
}
