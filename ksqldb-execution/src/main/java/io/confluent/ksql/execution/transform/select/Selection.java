/*
 * Copyright 2022 Confluent Inc.
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

package io.confluent.ksql.execution.transform.select;

import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.execution.plan.SelectExpression;
import io.confluent.ksql.execution.transform.select.SelectValueMapper.SelectInfo;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.util.KsqlConfig;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public final class Selection<K> {

  private final SelectValueMapper<K> mapper;
  private final LogicalSchema schema;

  public static <K> Selection<K> of(
      final LogicalSchema sourceSchema,
      final List<ColumnName> keyColumnNames,
      final Optional<ImmutableList<ColumnName>> selectedKeys,
      final List<SelectExpression> selectExpressions,
      final KsqlConfig ksqlConfig,
      final FunctionRegistry functionRegistry
  ) {
    final SelectValueMapper<K> mapper = SelectValueMapperFactory.create(
        selectExpressions,
        sourceSchema,
        ksqlConfig,
        functionRegistry
    );

    final LogicalSchema schema = buildSchema(sourceSchema, mapper, keyColumnNames, selectedKeys);
    return new Selection<>(mapper, schema);
  }

  private static LogicalSchema buildSchema(
      final LogicalSchema sourceSchema,
      final SelectValueMapper<?> mapper,
      final List<ColumnName> keyColumnNames,
      final Optional<ImmutableList<ColumnName>> selectedKeys
  ) {
    final List<ColumnName> keyNames = keyColumnNames == null || keyColumnNames.isEmpty()
        ? getKeyColumnNames(sourceSchema)
        : keyColumnNames;

    final List<Column> keyCols = sourceSchema.key();

    final LogicalSchema.Builder schemaBuilder = LogicalSchema.builder();
    final ImmutableList<ColumnName> selectedKeyColumns = selectedKeys.orElse(keyCols.stream()
        .map(Column::name)
        .collect(ImmutableList.toImmutableList()));

    for (int i = 0; i != keyCols.size(); ++i) {
      if (selectedKeyColumns.contains(keyCols.get(i).name())) {
        schemaBuilder.keyColumn(
            keyNames.get(i),
            keyCols.get(i).type()
        );
      }
    }

    for (final SelectInfo select : mapper.getSelects()) {
      schemaBuilder.valueColumn(select.getFieldName(), select.getEvaluator().getExpressionType());
    }

    return schemaBuilder.build();
  }

  private static List<ColumnName> getKeyColumnNames(final LogicalSchema sourceSchema) {
    return sourceSchema.key().stream()
        .map(Column::name)
        .collect(Collectors.toList());
  }

  private Selection(final SelectValueMapper<K> mapper, final LogicalSchema schema) {
    this.mapper = requireNonNull(mapper, "mapper");
    this.schema = requireNonNull(schema, "schema");
  }

  public SelectValueMapper<K> getMapper() {
    return mapper;
  }

  public LogicalSchema getSchema() {
    return schema;
  }
}
