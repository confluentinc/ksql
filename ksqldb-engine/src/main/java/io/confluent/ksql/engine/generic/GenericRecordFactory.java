/*
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.engine.generic;

import com.google.common.collect.Streams;
import io.confluent.ksql.GenericKey;
import io.confluent.ksql.GenericKey.Builder;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.metastore.model.DataSource.DataSourceType;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.SystemColumns;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

/**
 * Builds {@link KsqlGenericRecord} from lists of expressions/column names,
 * coercing any values to the correct java type using the schema of the supplied
 * {@link DataSource}.
 */
public class GenericRecordFactory {

  private final KsqlConfig config;
  private final FunctionRegistry functionRegistry;
  private final LongSupplier clock;

  public GenericRecordFactory(
      final KsqlConfig config,
      final FunctionRegistry functionRegistry,
      final LongSupplier clock
  ) {
    this.config = Objects.requireNonNull(config, "config");
    this.functionRegistry = Objects.requireNonNull(functionRegistry, "functionRegistry");
    this.clock = Objects.requireNonNull(clock, "clock");
  }

  public KsqlGenericRecord build(
      final List<ColumnName> columnNames,
      final List<Expression> expressions,
      final LogicalSchema schema,
      final DataSourceType dataSourceType
  ) {
    final List<ColumnName> columns = columnNames.isEmpty()
        ? implicitColumns(schema)
        : columnNames;

    if (columns.size() != expressions.size()) {
      throw new KsqlException(
          "Expected a value for each column."
              + " Expected Columns: " + columnNames
              + ". Got " + expressions);
    }

    final LogicalSchema schemaWithPseudoColumns = withPseudoColumns(schema);
    for (ColumnName col : columns) {

      if (!schemaWithPseudoColumns.findColumn(col).isPresent()) {
        throw new KsqlException("Column name " + col + " does not exist.");
      }

      if (SystemColumns.isDisallowedForInsertValues(col)) {
        throw new KsqlException("Inserting into column " + col + " is not allowed.");
      }
    }

    final Map<ColumnName, Object> values = resolveValues(
        columns,
        expressions,
        schemaWithPseudoColumns,
        functionRegistry,
        config
    );

    if (dataSourceType == DataSourceType.KTABLE) {
      final String noValue = schemaWithPseudoColumns.key().stream()
          .map(Column::name)
          .filter(colName -> !values.containsKey(colName))
          .map(ColumnName::text)
          .collect(Collectors.joining(", "));

      if (!noValue.isEmpty()) {
        throw new KsqlException("Value for primary key column(s) "
            + noValue + " is required for tables");
      }
    }

    final long ts = (long) values.getOrDefault(SystemColumns.ROWTIME_NAME, clock.getAsLong());

    final GenericKey key = buildKey(schema, values);
    final GenericRow value = buildValue(schema, values);

    return KsqlGenericRecord.of(key, value, ts);
  }

  @SuppressWarnings("UnstableApiUsage")
  private static List<ColumnName> implicitColumns(final LogicalSchema schema) {
    return Streams.concat(
        schema.key().stream(),
        schema.value().stream())
        .map(Column::name)
        .collect(Collectors.toList());
  }

  private static LogicalSchema withPseudoColumns(
      final LogicalSchema schema) {
    // The set of columns users can supply values for includes pseudocolumns,
    // so include them in the schema

    final LogicalSchema.Builder builder = schema.asBuilder();

    builder.valueColumn(SystemColumns.ROWTIME_NAME, SystemColumns.ROWTIME_TYPE);
    builder.valueColumn(SystemColumns.ROWPARTITION_NAME, SystemColumns.ROWPARTITION_TYPE);
    builder.valueColumn(SystemColumns.ROWOFFSET_NAME, SystemColumns.ROWOFFSET_TYPE);

    return builder.build();
  }

  private static Map<ColumnName, Object> resolveValues(
      final List<ColumnName> columns,
      final List<Expression> expressions,
      final LogicalSchema schema,
      final FunctionRegistry functionRegistry,
      final KsqlConfig config
  ) {
    final Map<ColumnName, Object> values = new HashMap<>();
    for (int i = 0; i < columns.size(); i++) {
      final ColumnName column = columns.get(i);
      final SqlType columnType = columnType(column, schema);
      final Expression valueExp = expressions.get(i);

      final Object value = new GenericExpressionResolver(
          columnType,
          column,
          functionRegistry,
          config,
          "insert value",
          false).resolve(valueExp);

      values.put(column, value);
    }
    return values;
  }

  private static SqlType columnType(final ColumnName column, final LogicalSchema schema) {
    return schema
        .findColumn(column)
        .map(Column::type)
        .orElseThrow(IllegalStateException::new);
  }

  private static GenericKey buildKey(
      final LogicalSchema schema,
      final Map<ColumnName, Object> values
  ) {
    final Builder builder = GenericKey.builder(schema);

    schema.key().stream()
        .map(Column::name)
        .map(values::get)
        .forEach(builder::append);

    return builder.build();
  }

  private static GenericRow buildValue(
      final LogicalSchema schema,
      final Map<ColumnName, Object> values
  ) {
    return new GenericRow().appendAll(
        schema
            .value()
            .stream()
            .map(Column::name)
            .map(values::get)
            .collect(Collectors.toList())
    );
  }

}
