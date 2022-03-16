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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.Range;
import io.confluent.ksql.GenericKey;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.streams.materialization.MaterializationException;
import io.confluent.ksql.execution.streams.materialization.MaterializedWindowedTable;
import io.confluent.ksql.execution.streams.materialization.StreamsMaterializedWindowedTable;
import io.confluent.ksql.execution.streams.materialization.WindowedRow;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Window;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.query.FailureReason;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.query.StateQueryRequest;
import org.apache.kafka.streams.query.StateQueryResult;
import org.apache.kafka.streams.query.WindowRangeQuery;
import org.apache.kafka.streams.state.KeyValueIterator;

/**
 * Kafka Streams impl of {@link MaterializedWindowedTable}.
 */
class KsMaterializedSessionTableIQv2 implements StreamsMaterializedWindowedTable {

  private final KsStateStore stateStore;

  KsMaterializedSessionTableIQv2(final KsStateStore store) {
    this.stateStore = Objects.requireNonNull(store, "store");
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
      final WindowRangeQuery<GenericKey, GenericRow> query = WindowRangeQuery.withKey(key);
      StateQueryRequest<KeyValueIterator<Windowed<GenericKey>, GenericRow>> request =
          inStore(stateStore.getStateStoreName()).withQuery(query);
      if (position.isPresent()) {
        request = request.withPositionBound(PositionBound.at(position.get()));
      }
      final StateQueryResult<KeyValueIterator<Windowed<GenericKey>, GenericRow>> result =
          stateStore.getKafkaStreams().query(request);

      final QueryResult<KeyValueIterator<Windowed<GenericKey>, GenericRow>> queryResult =
          result.getPartitionResults().get(partition);

      if (queryResult.isFailure()) {
        throw failedQueryException(queryResult);
      }

      try (KeyValueIterator<Windowed<GenericKey>, GenericRow> it =
          queryResult.getResult()) {

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
        return KsMaterializedQueryResult.rowIteratorWithPosition(
            builder.build().iterator(), queryResult.getPosition());
      }
    } catch (final NotUpToBoundException | MaterializationException e) {
      throw e;
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

  private Exception failedQueryException(final QueryResult<?> queryResult) {
    final String message = "Failed to get value from materialized table: "
        + queryResult.getFailureReason() + ": " + queryResult.getFailureMessage();

    if (queryResult.getFailureReason().equals(FailureReason.NOT_UP_TO_BOUND)) {
      return new NotUpToBoundException(message);
    } else {
      return new MaterializationException(message);
    }
  }
}
