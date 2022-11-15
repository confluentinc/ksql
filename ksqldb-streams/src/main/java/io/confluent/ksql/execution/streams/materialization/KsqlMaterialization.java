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
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.transform.KsqlProcessingContext;
import io.confluent.ksql.model.WindowType;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.apache.kafka.connect.data.Struct;

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
    public Optional<Row> get(final Struct key, final int partition) {
      return table.get(key, partition)
          .flatMap(row -> filterAndTransform(key, row.value(), row.rowTime())
              .map(v -> row.withValue(v, schema()))
          );
    }
  }

  final class KsqlMaterializedWindowedTable implements MaterializedWindowedTable {

    private final MaterializedWindowedTable table;

    KsqlMaterializedWindowedTable(final MaterializedWindowedTable table) {
      this.table = requireNonNull(table, "table'");
    }

    @Override
    public List<WindowedRow> get(
        final Struct key,
        final int partition,
        final Range<Instant> windowStart,
        final Range<Instant> windowEnd
    ) {
      final List<WindowedRow> result = table.get(key, partition, windowStart, windowEnd);

      final Builder<WindowedRow> builder = ImmutableList.builder();

      for (final WindowedRow row : result) {
        filterAndTransform(row.windowedKey(), row.value(), row.rowTime())
            .ifPresent(v -> builder.add(row.withValue(v, schema())));
      }

      return builder.build();
    }
  }
}

