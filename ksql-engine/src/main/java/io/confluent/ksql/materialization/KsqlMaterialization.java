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

package io.confluent.ksql.materialization;

import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.Range;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.model.WindowType;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.kstream.Predicate;

/**
 * {@link Materialization} implementation responsible for handling HAVING and SELECT clauses.
 *
 * <p>Underlying {@link Materialization} store data is not the same as the table it servers.
 * Specifically, it has not had:
 * <ol>
 * <li>
 *   The {@link io.confluent.ksql.function.udaf.Udaf#map} call applied to convert intermediate
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
  private final Function<GenericRow, GenericRow> aggregateTransform;
  private final Predicate<Struct, GenericRow> havingPredicate;
  private final Function<GenericRow, GenericRow> storeToTableTransform;
  private final LogicalSchema schema;

  /**
   * @param inner the inner materialization, e.g. a KS specific one
   * @param aggregateTransform converts from aggregates from intermediate to output types.
   * @param havingPredicate the predicate for handling HAVING clauses.
   * @param storeToTableTransform maps from internal to table schema.
   * @param schema the schema of the materialized table.
   */
  KsqlMaterialization(
      final Materialization inner,
      final Function<GenericRow, GenericRow> aggregateTransform,
      final Predicate<Struct, GenericRow> havingPredicate,
      final Function<GenericRow, GenericRow> storeToTableTransform,
      final LogicalSchema schema
  ) {
    this.inner = requireNonNull(inner, "table");
    this.aggregateTransform = requireNonNull(aggregateTransform, "aggregateTransform");
    this.havingPredicate = requireNonNull(havingPredicate, "havingPredicate");
    this.storeToTableTransform = requireNonNull(storeToTableTransform, "storeToTableTransform");
    this.schema = requireNonNull(schema, "schema");
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
      final Struct key,
      final GenericRow value
  ) {
    return Optional.of(value)
        // Call Udaf.map() to convert the internal representation stored in the state store into
        // the output type of the aggregator
        .map(aggregateTransform)
        // HAVING predicate from source table query that has not already been applied to the
        // store, so must be applied to any result from the store.
        .filter(v -> havingPredicate.test(key, v))
        // SELECTS that map from the stores internal schema to the tables true schema
        // i.e. maps from internal schema of the store to external schema of the table:
        .map(storeToTableTransform);
  }

  final class KsqlMaterializedTable implements MaterializedTable {

    private final MaterializedTable table;

    KsqlMaterializedTable(final MaterializedTable table) {
      this.table = requireNonNull(table, "table'");
    }

    @Override
    public Optional<Row> get(final Struct key) {
      return table.get(key)
          .flatMap(row -> filterAndTransform(key, row.value())
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
    public List<WindowedRow> get(final Struct key, final Range<Instant> windowStart) {
      final List<WindowedRow> result = table.get(key, windowStart);

      final Builder<WindowedRow> builder = ImmutableList.builder();

      for (final WindowedRow row : result) {
        filterAndTransform(key, row.value())
            .ifPresent(v -> builder.add(row.withValue(v, schema())));
      }

      return builder.build();
    }
  }
}

