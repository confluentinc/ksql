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

import static org.apache.kafka.streams.query.StateQueryRequest.inStore;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Streams;
import io.confluent.ksql.GenericKey;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.streams.materialization.MaterializationException;
import io.confluent.ksql.execution.streams.materialization.MaterializedTable;
import io.confluent.ksql.execution.streams.materialization.Row;
import io.confluent.ksql.util.IteratorUtil;
import java.util.Iterator;
import java.util.Objects;
import java.util.Optional;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.query.RangeQuery;
import org.apache.kafka.streams.query.StateQueryRequest;
import org.apache.kafka.streams.query.StateQueryResult;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;

/**
 * Kafka Streams impl of {@link MaterializedTable}.
 */
class KsMaterializedTable implements MaterializedTable {

  private final KsStateStore stateStore;

  KsMaterializedTable(final KsStateStore store) {
    this.stateStore = Objects.requireNonNull(store, "store");
  }

  @Override
  public Optional<Row> get(
      final GenericKey key,
      final int partition
  ) {
    try {
      final ReadOnlyKeyValueStore<GenericKey, ValueAndTimestamp<GenericRow>> store = stateStore
          .store(QueryableStoreTypes.timestampedKeyValueStore(), partition);

      return Optional.ofNullable(store.get(key))
          .map(v -> Row.of(stateStore.schema(), key, v.value(), v.timestamp()));
    } catch (final Exception e) {
      throw new MaterializationException("Failed to get value from materialized table", e);
    }
  }

  @Override
  public Iterator<Row> get(final int partition) {
    try {
      final RangeQuery<GenericKey, ValueAndTimestamp<GenericRow>> query = RangeQuery.withNoBounds();
      final StateQueryRequest<KeyValueIterator<GenericKey, ValueAndTimestamp<GenericRow>>>
          request = inStore(stateStore.getStateStoreName())
          .withQuery(query)
          .withPartitions(ImmutableSet.of(partition));
      final StateQueryResult<KeyValueIterator<GenericKey, ValueAndTimestamp<GenericRow>>>
          result = stateStore.getKafkaStreams().query(request);
      final KeyValueIterator<GenericKey, ValueAndTimestamp<GenericRow>> iterator =
          result.getOnlyPartitionResult().getResult();
      
      return Streams.stream(IteratorUtil.onComplete(iterator, iterator::close))
          .map(keyValue -> Row.of(stateStore.schema(), keyValue.key, keyValue.value.value(),
                                  keyValue.value.timestamp()))
          .iterator();
    } catch (final Exception e) {
      throw new MaterializationException("Failed to scan materialized table", e);
    }
  }

  @Override
  public Iterator<Row> get(final int partition, final GenericKey from, final GenericKey to) {
    try {
      final RangeQuery<GenericKey, ValueAndTimestamp<GenericRow>> query;
      if (from != null && to != null) {
        query = RangeQuery.withRange(from, to);
      } else if (from == null && to != null) {
        query = RangeQuery.withUpperBound(to);
      } else if (from != null && to == null) {
        query = RangeQuery.withLowerBound(from);
      } else {
        query = RangeQuery.withNoBounds();
      }

      final StateQueryRequest<KeyValueIterator<GenericKey, ValueAndTimestamp<GenericRow>>>
          request = inStore(stateStore.getStateStoreName())
          .withQuery(query)
          .withPartitions(ImmutableSet.of(partition));
      final StateQueryResult<KeyValueIterator<GenericKey, ValueAndTimestamp<GenericRow>>>
          result = stateStore.getKafkaStreams().query(request);
      final KeyValueIterator<GenericKey, ValueAndTimestamp<GenericRow>> iterator =
          result.getOnlyPartitionResult().getResult();

      return Streams.stream(IteratorUtil.onComplete(iterator, iterator::close))
          .map(keyValue -> Row.of(stateStore.schema(), keyValue.key, keyValue.value.value(),
                                  keyValue.value.timestamp()))
          .iterator();
    } catch (final Exception e) {
      throw new MaterializationException("Failed to range scan materialized table", e);
    }
  }

}
