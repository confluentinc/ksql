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
import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

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

  private final Materialization inner;
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
      final Materialization inner,
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

  final class KsqlMaterializedTable implements MaterializedTable {

    private final MaterializedTable table;

    KsqlMaterializedTable(final MaterializedTable table) {
      this.table = requireNonNull(table, "table'");
    }

    @Override
    public KsMaterializedQueryResult<Row> get(final GenericKey key, final int partition) {
      if (transforms.isEmpty()) {
        return table.get(key, partition);
      }

      final Iterator<Row> result = table.get(key, partition).getRowIterator();

      return KsMaterializedQueryResult.rowIterator(
          Streams.stream(result)
              .map(row -> filterAndTransform(row.key(), getIntermediateRow(row), row.rowTime())
                  .map(v -> row.withValue(v, schema())))
              .filter(Optional::isPresent)
              .map(Optional::get)
              .iterator());
    }

    @Override
    public KsMaterializedQueryResult<Row> get(final int partition) {
      if (transforms.isEmpty()) {
        return table.get(partition);
      }

      final Iterator<Row> result = table.get(partition).getRowIterator();

      return KsMaterializedQueryResult.rowIterator(
          Streams.stream(result)
              .map(row -> filterAndTransform(row.key(), getIntermediateRow(row), row.rowTime())
                  .map(v -> row.withValue(v, schema())))
              .filter(Optional::isPresent)
              .map(Optional::get)
              .iterator());
    }

    @Override
    public KsMaterializedQueryResult<Row> get(
        final int partition, final GenericKey from, final GenericKey to) {
      if (transforms.isEmpty()) {
        return table.get(partition, from, to);
      }

      final Iterator<Row> result = table.get(partition, from, to).getRowIterator();

      return KsMaterializedQueryResult.rowIterator(
          Streams.stream(result)
              .map(row -> filterAndTransform(row.key(), getIntermediateRow(row), row.rowTime())
                  .map(v -> row.withValue(v, schema())))
              .filter(Optional::isPresent)
              .map(Optional::get)
              .iterator());
    }
  }

  final class KsqlMaterializedWindowedTable implements MaterializedWindowedTable {

    private final MaterializedWindowedTable table;

    KsqlMaterializedWindowedTable(final MaterializedWindowedTable table) {
      this.table = requireNonNull(table, "table'");
    }

    @Override
    public KsMaterializedQueryResult<WindowedRow> get(
        final GenericKey key,
        final int partition,
        final Range<Instant> windowStart,
        final Range<Instant> windowEnd
    ) {
      if (transforms.isEmpty()) {
        return table.get(key, partition, windowStart, windowEnd);
      }

      final Iterator<WindowedRow> iterator = table.get(key, partition, windowStart, windowEnd)
          .getRowIterator();

      final Builder<WindowedRow> builder = ImmutableList.builder();

      while (iterator.hasNext()) {
        final WindowedRow row = iterator.next();
        filterAndTransform(row.windowedKey(), getIntermediateRow(row), row.rowTime())
            .ifPresent(v -> builder.add(row.withValue(v, schema())));
      }

      return KsMaterializedQueryResult.rowIterator(builder.build().iterator());
    }

    @Override
    public KsMaterializedQueryResult<WindowedRow> get(final int partition,
                                         final Range<Instant> windowStartBounds,
                                         final Range<Instant> windowEndBounds
    ) {
      if (transforms.isEmpty()) {
        return table.get(partition, windowStartBounds, windowEndBounds);
      }

      final Iterator<WindowedRow> result = table.get(partition, windowStartBounds, windowEndBounds)
          .getRowIterator();

      return KsMaterializedQueryResult.rowIterator(
          Streams.stream(result)
              .map(row -> filterAndTransform(row.windowedKey(),
                                             getIntermediateRow(row),
                                             row.rowTime())
                  .map(v -> row.withValue(v, schema())))
              .filter(Optional::isPresent)
              .map(Optional::get)
              .iterator());
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

