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

package io.confluent.ksql.execution.streams.materialization;

import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.Range;
import com.google.common.collect.Streams;
import io.confluent.ksql.GenericKey;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.streams.materialization.ks.KsMaterializedQueryResult;
import io.confluent.ksql.execution.transform.KsqlProcessingContext;
import io.confluent.ksql.model.WindowType;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.util.ConsistencyOffsetVector;
import io.confluent.ksql.util.ConsistencyUtil;
import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.apache.kafka.streams.query.Position;

/**
 * {@link Materialization} implementation responsible for handling HAVING and SELECT clauses.
 *
 * <p>Underlying {@link Materialization} store data is not the same as the table it servers.
 * Specifically, it has not had:
 * <ol>
 * <li>
 *   The io.confluent.ksql.function.udaf.Udaf#map call applied to convert intermediate
 *   aggregate types on output types
 * </li>
 * <li>
 *   Any HAVING predicate applied.
 * </li>
 * <li>
 *   The select value mapper applied to convert from the internal schema to the table's scheam.
 * </li>
 * </ol>
 *
 * <p>This class is responsible for this for now. Long term, these should be handled by physical
 * plan steps.
 */
class KsqlMaterialization implements Materialization {

  private final StreamsMaterialization inner;
  private final LogicalSchema schema;

  private final List<Transform> transforms;

  interface Transform {

    Optional<GenericRow> apply(Object key, GenericRow value, KsqlProcessingContext ctx);
  }

  /**
   * @param inner the inner materialization, e.g. a KS specific one
   * @param schema the schema of the materialized table.
   * @param transforms list of transforms to apply
   */
  KsqlMaterialization(
      final StreamsMaterialization inner,
      final LogicalSchema schema,
      final List<Transform> transforms
  ) {
    this.inner = requireNonNull(inner, "table");
    this.schema = requireNonNull(schema, "schema");
    this.transforms = ImmutableList.copyOf(
        Objects.requireNonNull(transforms, "transforms"));
  }

  @Override
  public LogicalSchema schema() {
    return schema;
  }

  @Override
  public Locator locator() {
    return inner.locator();
  }

  @Override
  public Optional<WindowType> windowType() {
    return inner.windowType();
  }

  @Override
  public MaterializedTable nonWindowed() {
    return new KsqlMaterializedTable(inner.nonWindowed());
  }

  @Override
  public MaterializedWindowedTable windowed() {
    return new KsqlMaterializedWindowedTable(inner.windowed());
  }

  private Optional<GenericRow> filterAndTransform(
      final Object key,
      final GenericRow value,
      final long rowTime
  ) {
    GenericRow intermediate = value;
    for (final Transform transform : transforms) {
      final Optional<GenericRow> result = transform.apply(
          key,
          intermediate,
          new PullProcessingContext(rowTime)
      );

      if (!result.isPresent()) {
        return Optional.empty();
      }
      intermediate = result.get();
    }
    return Optional.of(intermediate);
  }

  private void updateConsistencyVector(
      final Optional<ConsistencyOffsetVector> consistencyVector,
      final Optional<Position> position
  ) {
    if (position.isPresent() && consistencyVector.isPresent()) {
      ConsistencyUtil.updateFromPosition(consistencyVector.get(), position.get());
    }
  }

  final class KsqlMaterializedTable implements MaterializedTable {

    private final StreamsMaterializedTable table;

    KsqlMaterializedTable(final StreamsMaterializedTable table) {
      this.table = requireNonNull(table, "table'");
    }

    @Override
    public Iterator<Row> get(
        final GenericKey key,
        final int partition,
        final Optional<ConsistencyOffsetVector> consistencyVector
    ) {
      final Optional<Position> position = consistencyVector
          .map(offsetVector -> Position.fromMap(offsetVector.getOffsetVector()));
      final KsMaterializedQueryResult<Row> result = table.get(key, partition, position);
      updateConsistencyVector(consistencyVector, result.getPosition());

      if (transforms.isEmpty()) {
        return result.getRowIterator();
      } else {
        final Iterator<Row> iterator = Streams.stream(result.getRowIterator())
            .map(row -> filterAndTransform(row.key(), getIntermediateRow(row), row.rowTime())
                .map(v -> row.withValue(v, schema())))
            .filter(Optional::isPresent)
            .map(Optional::get)
            .iterator();
        return iterator;
      }
    }

    @Override
    public Iterator<Row> get(
        final int partition,
        final Optional<ConsistencyOffsetVector> consistencyVector
    ) {
      final Optional<Position> position = consistencyVector
          .map(offsetVector -> Position.fromMap(offsetVector.getOffsetVector()));
      final KsMaterializedQueryResult<Row> result = table.get(partition, position);
      updateConsistencyVector(consistencyVector, result.getPosition());

      if (transforms.isEmpty()) {
        return result.getRowIterator();
      } else {
        final Iterator<Row> iterator = Streams.stream(result.getRowIterator())
            .map(row -> filterAndTransform(row.key(), getIntermediateRow(row), row.rowTime())
                .map(v -> row.withValue(v, schema())))
            .filter(Optional::isPresent)
            .map(Optional::get)
            .iterator();
        return iterator;
      }
    }

    @Override
    public Iterator<Row> get(
        final int partition,
        final GenericKey from,
        final GenericKey to,
        final Optional<ConsistencyOffsetVector> consistencyVector
    ) {
      final Optional<Position> position = consistencyVector
          .map(offsetVector -> Position.fromMap(offsetVector.getOffsetVector()));
      final KsMaterializedQueryResult<Row> result = table.get(partition, from, to, position);
      updateConsistencyVector(consistencyVector, result.getPosition());

      if (transforms.isEmpty()) {
        return result.getRowIterator();
      } else {
        final Iterator<Row> iterator = Streams.stream(result.getRowIterator())
            .map(row -> filterAndTransform(row.key(), getIntermediateRow(row), row.rowTime())
                .map(v -> row.withValue(v, schema())))
            .filter(Optional::isPresent)
            .map(Optional::get)
            .iterator();
        return iterator;
      }
    }
  }

  final class KsqlMaterializedWindowedTable implements MaterializedWindowedTable {

    private final StreamsMaterializedWindowedTable table;

    KsqlMaterializedWindowedTable(final StreamsMaterializedWindowedTable table) {
      this.table = requireNonNull(table, "table'");
    }

    @Override
    public Iterator<WindowedRow> get(
        final GenericKey key,
        final int partition,
        final Range<Instant> windowStart,
        final Range<Instant> windowEnd,
        final Optional<ConsistencyOffsetVector> consistencyVector
    ) {
      final Optional<Position> position = consistencyVector
          .map(offsetVector -> Position.fromMap(offsetVector.getOffsetVector()));
      final KsMaterializedQueryResult<WindowedRow> result = table.get(
          key, partition, windowStart, windowEnd, position);
      updateConsistencyVector(consistencyVector, result.getPosition());

      if (transforms.isEmpty()) {
        return result.getRowIterator();
      } else {
        final Builder<WindowedRow> builder = ImmutableList.builder();

        while (result.getRowIterator().hasNext()) {
          final WindowedRow row = result.getRowIterator().next();
          filterAndTransform(row.windowedKey(), getIntermediateRow(row), row.rowTime())
              .ifPresent(v -> builder.add(row.withValue(v, schema())));
        }
        return builder.build().iterator();
      }
    }

    @Override
    public Iterator<WindowedRow> get(
        final int partition,
        final Range<Instant> windowStartBounds,
        final Range<Instant> windowEndBounds,
        final Optional<ConsistencyOffsetVector> consistencyVector
    ) {
      final Optional<Position> position = consistencyVector
          .map(offsetVector -> Position.fromMap(offsetVector.getOffsetVector()));
      final KsMaterializedQueryResult<WindowedRow> result = table.get(
          partition, windowStartBounds, windowEndBounds, position);
      updateConsistencyVector(consistencyVector, result.getPosition());

      if (transforms.isEmpty()) {
        return result.getRowIterator();
      } else {
        final Iterator<WindowedRow> iterator = Streams.stream(result.getRowIterator())
            .map(row -> filterAndTransform(row.windowedKey(),
                                           getIntermediateRow(row),
                                           row.rowTime())
                .map(v -> row.withValue(v, schema())))
            .filter(Optional::isPresent)
            .map(Optional::get)
            .iterator();
        return iterator;
      }
    }
  }

  /*
   Today, we are unconditionally adding the extra fields to windowed rows.
   We should decide if we need these additional fields for the
   Windowed Rows case and remove them if possible.
   */
  public static GenericRow getIntermediateRow(final TableRow row) {
    final GenericKey key = row.key();
    final GenericRow value = row.value();

    final List<?> keyFields = key.values();

    value.ensureAdditionalCapacity(
        1 // ROWTIME
            + keyFields.size() //all the keys
            + row.window().map(w -> 2).orElse(0) //windows
    );

    value.append(row.rowTime());
    value.appendAll(keyFields);

    row.window().ifPresent(window -> {
      value.append(window.start().toEpochMilli());
      value.append(window.end().toEpochMilli());
    });

    return value;
  }
}

