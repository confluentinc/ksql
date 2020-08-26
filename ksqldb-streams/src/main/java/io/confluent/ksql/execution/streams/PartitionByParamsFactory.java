/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.execution.streams;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.codegen.CodeGenRunner;
import io.confluent.ksql.execution.codegen.ExpressionMetadata;
import io.confluent.ksql.execution.expression.tree.ColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.NullLiteral;
import io.confluent.ksql.execution.util.ExpressionTypeManager;
import io.confluent.ksql.execution.util.StructKeyUtil;
import io.confluent.ksql.execution.util.StructKeyUtil.KeyBuilder;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.ColumnNames;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.LogicalSchema.Builder;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.KeyValue;

/**
 * Factory for PartitionByParams.
 *
 * <p>Behaviour differs slightly depending on whether PARTITIONing BY a column reference or some
 * other expression:
 *
 * <p>When PARTITIONing BY a column reference the existing key column(s) are moved into the value
 * schema and the new key column is moved to the key, e.g. logically {@code A => B, C}, when
 * {@code PARTITION BY B}, becomes {@code B => C, A}.  However, processing schemas contain a copy of
 * the key columns in the value, so actually {@code A => B, C, A} becomes {@code B => B, C, A}:
 * Note: the value columns does not need to change.
 *
 * <p>When PARTITIONing BY any other type of expression no column can be removed from the logical
 * schema's value columns. The PARTITION BY expression is creating a <i>new</i> column (except in
 * the case of PARTITION BY NULL -- see below). Hence, the existing key column(s) are moved to the
 * value schema and a <i>new</i> key column is added, e.g. logically {@code A => B, C}, when
 * {@code PARTITION BY exp}, becomes {@code KSQL_COL_0 => B, C, A}. However, processing schemas
 * contain a copy of the key columns in the value, so actually {@code A => B, C, A} becomes
 * {@code KSQL_COL_0 => B, C, A, KSQL_COL_0}. Note: the value column only has the new key column
 * added.
 *
 * <p>When PARTITIONing BY NULL, the existing key column(ns) are moved into the value schema and
 * the new key is null. Because processing schemas contain a copy of the key columns in the value,
 * the value columns do not need to change. Instead, the key is just set to null.
 */
public final class PartitionByParamsFactory {

  private PartitionByParamsFactory() {
  }

  public static PartitionByParams build(
      final LogicalSchema sourceSchema,
      final Expression partitionBy,
      final KsqlConfig ksqlConfig,
      final FunctionRegistry functionRegistry,
      final ProcessingLogger logger
  ) {
    final Optional<ColumnName> partitionByCol = getPartitionByColumnName(sourceSchema, partitionBy);

    final LogicalSchema resultSchema =
        buildSchema(sourceSchema, partitionBy, functionRegistry, partitionByCol);

    final BiFunction<Object, GenericRow, KeyValue<Struct, GenericRow>> mapper;
    if (partitionBy instanceof NullLiteral) {
      // In case of PARTITION BY NULL, it is sufficient to set the new key to null as the old key
      // is already present in the current value
      mapper = (k, v) -> new KeyValue<>(null, v);
    } else {
      final Function<GenericRow, Object> evaluator = buildExpressionEvaluator(
          sourceSchema,
          partitionBy,
          ksqlConfig,
          functionRegistry,
          logger
      );
      mapper = buildMapper(resultSchema, partitionByCol, evaluator);
    }

    return new PartitionByParams(resultSchema, mapper);
  }

  public static LogicalSchema buildSchema(
      final LogicalSchema sourceSchema,
      final Expression partitionBy,
      final FunctionRegistry functionRegistry
  ) {
    final Optional<ColumnName> partitionByCol =
        getPartitionByColumnName(sourceSchema, partitionBy);

    return buildSchema(sourceSchema, partitionBy, functionRegistry, partitionByCol);
  }

  private static LogicalSchema buildSchema(
      final LogicalSchema sourceSchema,
      final Expression partitionBy,
      final FunctionRegistry functionRegistry,
      final Optional<ColumnName> partitionByCol
  ) {
    final ExpressionTypeManager expressionTypeManager =
        new ExpressionTypeManager(sourceSchema, functionRegistry);

    final SqlType keyType = expressionTypeManager
        .getExpressionSqlType(partitionBy);

    final ColumnName newKeyName = partitionByCol
        .orElseGet(() -> ColumnNames.uniqueAliasFor(partitionBy, sourceSchema));

    final Builder builder = LogicalSchema.builder();
    if (keyType != null) {
      builder.keyColumn(newKeyName, keyType);
    }
    builder.valueColumns(sourceSchema.value());

    if (keyType != null && !partitionByCol.isPresent()) {
      // New key column added, copy in to value schema:
      builder.valueColumn(newKeyName, keyType);
    }

    return builder.build();
  }

  private static Optional<ColumnName> getPartitionByColumnName(
      final LogicalSchema sourceSchema,
      final Expression partitionBy
  ) {
    if (partitionBy instanceof ColumnReferenceExp) {
      // PARTITION BY column:
      final ColumnName columnName = ((ColumnReferenceExp) partitionBy).getColumnName();

      final Column column = sourceSchema
          .findValueColumn(columnName)
          .orElseThrow(() ->
              new IllegalStateException("Unknown partition by column: " + columnName));

      return Optional.of(column.name());
    }

    return Optional.empty();
  }

  private static BiFunction<Object, GenericRow, KeyValue<Struct, GenericRow>> buildMapper(
      final LogicalSchema resultSchema,
      final Optional<ColumnName> partitionByCol,
      final Function<GenericRow, Object> evaluator
  ) {
    // If partitioning by something other than an existing column, then a new key will have
    // been synthesized. This new key must be appended to the value to make it available for
    // stream processing, in the same way SourceBuilder appends the key and rowtime to the value:
    final boolean appendNewKey = !partitionByCol.isPresent();

    final KeyBuilder keyBuilder = StructKeyUtil.keyBuilder(resultSchema);

    return (k, v) -> {
      final Object newKey = evaluator.apply(v);
      final Struct structKey = keyBuilder.build(newKey);

      if (appendNewKey) {
        v.append(newKey);
      }

      return new KeyValue<>(structKey, v);
    };
  }

  private static Function<GenericRow, Object> buildExpressionEvaluator(
      final LogicalSchema schema,
      final Expression partitionBy,
      final KsqlConfig ksqlConfig,
      final FunctionRegistry functionRegistry,
      final ProcessingLogger logger
  ) {
    final CodeGenRunner codeGen = new CodeGenRunner(
        schema,
        ksqlConfig,
        functionRegistry
    );

    final ExpressionMetadata expressionMetadata = codeGen
        .buildCodeGenFromParseTree(partitionBy, "SelectKey");

    final String errorMsg = "Error computing new key from expression "
        + expressionMetadata.getExpression();

    return row -> expressionMetadata.evaluate(row, null, logger, () -> errorMsg);
  }
}
