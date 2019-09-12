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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.model.WindowType;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import java.time.Instant;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.function.Function;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.kstream.Predicate;

/**
 * {@link Materialization} implementation responsible for handling HAVING and SELECT clauses.
 *
 * <p>Underlying {@link Materialization} store data in a different schema and have not had any
 * HAVING predicate applied.  Mapping from the aggregate store schema to the table's schema and
 * applying any HAVING predicate is handled by this class.
 */
class KsqlMaterialization implements Materialization {

  private final Materialization inner;
  private final Predicate<Struct, GenericRow> havingPredicate;
  private final Function<GenericRow, GenericRow> storeToTableTransform;
  private final LogicalSchema schema;

  KsqlMaterialization(
      final Materialization inner,
      final Predicate<Struct, GenericRow> havingPredicate,
      final Function<GenericRow, GenericRow> storeToTableTransform,
      final LogicalSchema schema
  ) {
    this.inner = requireNonNull(inner, "table");
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
      final GenericRow row
  ) {
    return Optional.of(row)
        // HAVING predicate from source table query that has not already been applied to the
        // store, so must be applied to any result from the store.
        .filter(value -> havingPredicate.test(key, value))
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
    public Optional<GenericRow> get(final Struct key) {
      return table.get(key)
          .flatMap(v -> filterAndTransform(key, v));
    }
  }

  class KsqlMaterializedWindowedTable implements MaterializedWindowedTable {

    private final MaterializedWindowedTable table;

    KsqlMaterializedWindowedTable(final MaterializedWindowedTable table) {
      this.table = requireNonNull(table, "table'");
    }

    @Override
    public Map<Window, GenericRow> get(final Struct key, final Instant lower, final Instant upper) {
      final Map<Window, GenericRow> result = table.get(key, lower, upper);

      final Builder<Window, GenericRow> builder = ImmutableMap.builder();

      for (final Entry<Window, GenericRow> e : result.entrySet()) {
        filterAndTransform(key, e.getValue())
            .ifPresent(v -> builder.put(e.getKey(), v));
      }

      return builder.build();
    }
  }
}

