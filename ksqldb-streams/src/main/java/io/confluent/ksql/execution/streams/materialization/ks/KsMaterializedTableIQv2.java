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
import java.util.Collections;
import java.util.Iterator;
import java.util.Objects;
import java.util.Optional;
import org.apache.kafka.streams.query.KeyQuery;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.query.RangeQuery;
import org.apache.kafka.streams.query.StateQueryRequest;
import org.apache.kafka.streams.query.StateQueryResult;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ValueAndTimestamp;

/**
 * Kafka Streams impl of {@link MaterializedTable}.
 */
class KsMaterializedTableIQv2 implements MaterializedTable {

  private final KsStateStore stateStore;

  KsMaterializedTableIQv2(final KsStateStore store) {
    this.stateStore = Objects.requireNonNull(store, "store");
  }

  @Override
  public Optional<Row> get(
      final GenericKey key,
      final int partition
  ) {
    try {
      final KeyQuery<GenericKey, ValueAndTimestamp<GenericRow>> query = KeyQuery.withKey(key);
      final StateQueryRequest<ValueAndTimestamp<GenericRow>>
          request = inStore(stateStore.getStateStoreName())
          .withQuery(query)
          .withPartitions(ImmutableSet.of(partition));
      final StateQueryResult<ValueAndTimestamp<GenericRow>>
          result = stateStore.getKafkaStreams().query(request);

      final QueryResult<ValueAndTimestamp<GenericRow>> queryResult =
          result.getPartitionResults().get(partition);
      // Some of these failures are retriable, and in the future, we may want to retry
      // locally before throwing.
      if (queryResult.isFailure()) {
        throw failedQueryException(queryResult);
      } else if (queryResult.getResult() == null) {
        return Optional.empty();
      } else {
        final ValueAndTimestamp<GenericRow> row = queryResult.getResult();
        return Optional.of(Row.of(stateStore.schema(), key, row.value(), row.timestamp()));
      }
    } catch (MaterializationException e) {
      throw e;
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

      final QueryResult<KeyValueIterator<GenericKey, ValueAndTimestamp<GenericRow>>> queryResult =
          result.getPartitionResults().get(partition);
      if (queryResult.isFailure()) {
        throw failedQueryException(queryResult);
      } else if (queryResult.getResult() == null) {
        return Collections.emptyIterator();
      } else {
        final KeyValueIterator<GenericKey, ValueAndTimestamp<GenericRow>> iterator =
            queryResult.getResult();
        return Streams.stream(IteratorUtil.onComplete(iterator, iterator::close))
            .map(keyValue -> Row.of(stateStore.schema(), keyValue.key, keyValue.value.value(),
                                    keyValue.value.timestamp()))
            .iterator();
      }
    } catch (MaterializationException e) {
      throw e;
    } catch (final Exception e) {
      throw new MaterializationException("Failed to scan materialized table", e);
    }
  }

  // CHECKSTYLE_RULES.OFF: CyclomaticComplexity
  @Override
  public Iterator<Row> get(final int partition, final GenericKey from, final GenericKey to) {
    // CHECKSTYLE_RULES.ON: CyclomaticComplexity
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

      final QueryResult<KeyValueIterator<GenericKey, ValueAndTimestamp<GenericRow>>> queryResult =
          result.getPartitionResults().get(partition);
      if (queryResult.isFailure()) {
        throw failedQueryException(queryResult);
      } else if (queryResult.getResult() == null) {
        return Collections.emptyIterator();
      } else {
        final KeyValueIterator<GenericKey, ValueAndTimestamp<GenericRow>> iterator =
            queryResult.getResult();
        return Streams.stream(IteratorUtil.onComplete(iterator, iterator::close))
            .map(keyValue -> Row.of(stateStore.schema(), keyValue.key, keyValue.value.value(),
                keyValue.value.timestamp()))
            .iterator();
      }
    } catch (MaterializationException e) {
      throw e;
    } catch (final Exception e) {
      throw new MaterializationException("Failed to range scan materialized table", e);
    }
  }

  private MaterializationException failedQueryException(final QueryResult<?> queryResult) {
    return new MaterializationException(
        "Failed to get value from materialized table: "
            + queryResult.getFailureReason() + ": "
            + queryResult.getFailureMessage()
    );
  }

}
