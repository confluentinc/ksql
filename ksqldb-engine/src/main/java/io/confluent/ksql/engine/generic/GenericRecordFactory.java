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
import org.apache.kafka.connect.data.Struct;

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

    final LogicalSchema schemaWithRowTime = withRowTime(schema);
    for (ColumnName col : columns) {
      if (!schemaWithRowTime.findColumn(col).isPresent()) {
        throw new KsqlException("Column name " + col + " does not exist.");
      }
    }

    final Map<ColumnName, Object> values = resolveValues(
        columns,
        expressions,
        schemaWithRowTime,
        functionRegistry,
        config
    );

    if (dataSourceType == DataSourceType.KTABLE) {
      final String noValue = schemaWithRowTime.key().stream()
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

    final Struct key = buildKey(schema, values);
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

  private static LogicalSchema withRowTime(final LogicalSchema schema) {
    // The set of columns users can supply values for includes the ROWTIME pseudocolumn,
    // so include it in the schema:
    return schema.asBuilder()
        .valueColumn(SystemColumns.ROWTIME_NAME, SystemColumns.ROWTIME_TYPE)
        .build();
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
          schema,
          functionRegistry,
          config
      ).resolve(valueExp);

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

  private static Struct buildKey(
      final LogicalSchema schema,
      final Map<ColumnName, Object> values
  ) {

    final Struct key = new Struct(schema.keyConnectSchema());

    for (final org.apache.kafka.connect.data.Field field : key.schema().fields()) {
      final Object value = values.get(ColumnName.of(field.name()));
      key.put(field, value);
    }

    return key;
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
