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
import com.google.common.collect.Streams;
import io.confluent.ksql.GenericKey;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.streams.materialization.MaterializationException;
import io.confluent.ksql.execution.streams.materialization.MaterializedWindowedTable;
import io.confluent.ksql.execution.streams.materialization.StreamsMaterializedWindowedTable;
import io.confluent.ksql.execution.streams.materialization.WindowedRow;
import io.confluent.ksql.util.IteratorUtil;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Objects;
import java.util.Optional;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.streams.query.FailureReason;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.query.StateQueryRequest;
import org.apache.kafka.streams.query.StateQueryResult;
import org.apache.kafka.streams.query.WindowKeyQuery;
import org.apache.kafka.streams.query.WindowRangeQuery;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.WindowStoreIterator;

/**
 * Kafka Streams impl of {@link MaterializedWindowedTable}.
 */
class KsMaterializedWindowTableIQv2 implements StreamsMaterializedWindowedTable {

  private final KsStateStore stateStore;
  private final Duration windowSize;

  KsMaterializedWindowTableIQv2(final KsStateStore store, final Duration windowSize) {
    this.stateStore = Objects.requireNonNull(store, "store");
    this.windowSize = Objects.requireNonNull(windowSize, "windowSize");
  }

  @Override
  public KsMaterializedQueryResult<WindowedRow> get(
      final GenericKey key,
      final int partition,
      final Range<Instant> windowStartBounds,
      final Range<Instant> windowEndBounds,
      final Optional<Position> position
  ) {
    try {
      final Instant lower = calculateLowerBound(windowStartBounds, windowEndBounds);
      final Instant upper = calculateUpperBound(windowStartBounds, windowEndBounds);
      final WindowKeyQuery<GenericKey, ValueAndTimestamp<GenericRow>> query =
          WindowKeyQuery.withKeyAndWindowStartRange(key, lower, upper);
      StateQueryRequest<WindowStoreIterator<ValueAndTimestamp<GenericRow>>> request =
          inStore(stateStore.getStateStoreName()).withQuery(query);
      if (position.isPresent()) {
        request = request.withPositionBound(PositionBound.at(position.get()));
      }
      final KafkaStreams streams = stateStore.getKafkaStreams();
      final StateQueryResult<WindowStoreIterator<ValueAndTimestamp<GenericRow>>> result =
          streams.query(request);

      final QueryResult<WindowStoreIterator<ValueAndTimestamp<GenericRow>>> queryResult =
          result.getPartitionResults().get(partition);

      if (queryResult.isFailure()) {
        throw failedQueryException(queryResult);
      }

      if (queryResult.getResult() == null) {
        return KsMaterializedQueryResult.rowIteratorWithPosition(
            Collections.emptyIterator(), queryResult.getPosition());
      }

      try (WindowStoreIterator<ValueAndTimestamp<GenericRow>> it
          = queryResult.getResult()) {

        final Builder<WindowedRow> builder = ImmutableList.builder();

        while (it.hasNext()) {
          final KeyValue<Long, ValueAndTimestamp<GenericRow>> next = it.next();

          final Instant windowStart = Instant.ofEpochMilli(next.key);
          if (!windowStartBounds.contains(windowStart)) {
            continue;
          }

          final Instant windowEnd = windowStart.plus(windowSize);
          if (!windowEndBounds.contains(windowEnd)) {
            continue;
          }

          final TimeWindow window =
              new TimeWindow(windowStart.toEpochMilli(), windowEnd.toEpochMilli());

          final WindowedRow row = WindowedRow.of(
              stateStore.schema(),
              new Windowed<>(key, window),
              next.value.value(),
              next.value.timestamp()
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

  public KsMaterializedQueryResult<WindowedRow> get(
      final int partition,
      final Range<Instant> windowStartBounds,
      final Range<Instant> windowEndBounds,
      final Optional<Position> position
  ) {
    try {
      final Instant lower = calculateLowerBound(windowStartBounds, windowEndBounds);
      final Instant upper = calculateUpperBound(windowStartBounds, windowEndBounds);

      final WindowRangeQuery<GenericKey, ValueAndTimestamp<GenericRow>> query =
          WindowRangeQuery.withWindowStartRange(lower, upper);
      StateQueryRequest<KeyValueIterator<Windowed<GenericKey>, ValueAndTimestamp<GenericRow>>>
          request = inStore(stateStore.getStateStoreName()).withQuery(query);
      if (position.isPresent()) {
        request = request.withPositionBound(PositionBound.at(position.get()));
      }
      final StateQueryResult<KeyValueIterator<Windowed<GenericKey>, ValueAndTimestamp<GenericRow>>>
          result = stateStore.getKafkaStreams().query(request);

      final QueryResult<KeyValueIterator<Windowed<GenericKey>,
          ValueAndTimestamp<GenericRow>>> queryResult = result.getPartitionResults().get(partition);

      if (queryResult.isFailure()) {
        throw failedQueryException(queryResult);
      }

      final KeyValueIterator<Windowed<GenericKey>, ValueAndTimestamp<GenericRow>> iterator =
          queryResult.getResult();

      return KsMaterializedQueryResult.rowIteratorWithPosition(
          Streams.stream(IteratorUtil.onComplete(iterator, iterator::close))
              .map(next -> {
                final Instant windowStart = next.key.window().startTime();
                if (!windowStartBounds.contains(windowStart)) {
                  return null;
                }

                final Instant windowEnd = next.key.window().endTime();
                if (!windowEndBounds.contains(windowEnd)) {
                  return null;
                }

                final TimeWindow window =
                    new TimeWindow(windowStart.toEpochMilli(), windowEnd.toEpochMilli());

                final WindowedRow row = WindowedRow.of(
                    stateStore.schema(),
                    new Windowed<>(next.key.key(), window),
                    next.value.value(),
                    next.value.timestamp()
                );

                return row; })
              .filter(Objects::nonNull)
              .iterator(),
          queryResult.getPosition()
      );
    } catch (final NotUpToBoundException | MaterializationException e) {
      throw e;
    } catch (final Exception e) {
      throw new MaterializationException("Failed to get value from materialized table", e);
    }
  }

  private Instant calculateUpperBound(
      final Range<Instant> windowStartBounds,
      final Range<Instant> windowEndBounds
  ) {
    final Instant start = windowStartBounds.hasUpperBound()
        ? windowStartBounds.upperEndpoint()
        : Instant.ofEpochMilli(Long.MAX_VALUE);

    final Instant end = windowEndBounds.hasUpperBound()
        ? windowEndBounds.upperEndpoint().minus(windowSize)
        : Instant.ofEpochMilli(Long.MAX_VALUE);

    return start.compareTo(end) < 0 ? start : end;
  }

  private Instant calculateLowerBound(
      final Range<Instant> windowStartBounds,
      final Range<Instant> windowEndBounds
  ) {
    final Instant start = windowStartBounds.hasLowerBound()
        ? windowStartBounds.lowerEndpoint()
        : Instant.ofEpochMilli(0);

    final Instant end = windowEndBounds.hasLowerBound()
        ? windowEndBounds.lowerEndpoint().minus(windowSize)
        : Instant.ofEpochMilli(0);

    return start.compareTo(end) < 0 ? end : start;
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
