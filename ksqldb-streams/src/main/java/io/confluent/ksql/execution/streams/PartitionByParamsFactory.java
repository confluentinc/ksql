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
import io.confluent.ksql.execution.util.ExpressionTypeManager;
import io.confluent.ksql.execution.util.StructKeyUtil;
import io.confluent.ksql.execution.util.StructKeyUtil.KeyBuilder;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.logging.processing.RecordProcessingError;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.ColumnNames;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.LogicalSchema.Builder;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
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
 * schema's value columns. The PARTITION BY expression is creating a <i>new</i> column. Hence, the
 * existing key column(s) are moved to the value schema and a <i>new</i> key column is added, e.g.
 * logically {@code A => B, C}, when {@code PARTITION BY exp}, becomes {@code KSQL_COL_0 => B, C, A}
 * However, processing schemas contain a copy of the key columns in the value, so actually {@code
 * A => B, C, A} becomes {@code KSQL_COL_0 => B, C, A, KSQL_COL_0}. Note: the value column only has
 * the new key column added.
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
    final Optional<Column> partitionByCol = getPartitionByCol(sourceSchema, partitionBy);

    final Function<GenericRow, Object> evaluator = buildExpressionEvaluator(
        sourceSchema,
        partitionBy,
        ksqlConfig,
        functionRegistry,
        logger
    );

    final LogicalSchema resultSchema =
        buildSchema(sourceSchema, partitionBy, functionRegistry, partitionByCol);

    return buildMapper(resultSchema, partitionByCol, evaluator);
  }

  public static LogicalSchema buildSchema(
      final LogicalSchema sourceSchema,
      final Expression partitionBy,
      final FunctionRegistry functionRegistry
  ) {
    final Optional<Column> partitionByCol = getPartitionByCol(sourceSchema, partitionBy);
    return buildSchema(sourceSchema, partitionBy, functionRegistry, partitionByCol);
  }

  private static LogicalSchema buildSchema(
      final LogicalSchema sourceSchema,
      final Expression partitionBy,
      final FunctionRegistry functionRegistry,
      final Optional<Column> partitionByCol
  ) {
    final ExpressionTypeManager expressionTypeManager =
        new ExpressionTypeManager(sourceSchema, functionRegistry);

    final SqlType keyType = expressionTypeManager
        .getExpressionSqlType(partitionBy);

    final ColumnName newKeyName = partitionByCol
        .map(Column::name)
        .orElseGet(() -> ColumnNames.nextGeneratedColumnAlias(sourceSchema));

    final Builder builder = LogicalSchema.builder()
        .withRowTime()
        .keyColumn(newKeyName, keyType)
        .valueColumns(sourceSchema.value());

    if (!partitionByCol.isPresent()) {
      // New key column added, copy in to value schema:
      builder.valueColumn(newKeyName, keyType);
    }

    return builder.build();
  }

  private static Optional<Column> getPartitionByCol(
      final LogicalSchema sourceSchema,
      final Expression partitionBy
  ) {
    if (!(partitionBy instanceof ColumnReferenceExp)) {
      return Optional.empty();
    }

    final ColumnName columnName = ((ColumnReferenceExp) partitionBy).getColumnName();

    final Column column = sourceSchema
        .findValueColumn(columnName)
        .orElseThrow(() -> new IllegalStateException("Unknown partition by column: " + columnName));

    return Optional.of(column);
  }

  private static PartitionByParams buildMapper(
      final LogicalSchema resultSchema,
      final Optional<Column> partitionByCol,
      final Function<GenericRow, Object> evaluator
  ) {
    // If partitioning by something other than an existing column, then a new key will have
    // been synthesized. This new key must be appended to the value to make it available for
    // stream processing, in the same way SourceBuilder appends the key and rowtime to the value:
    final boolean appendNewKey = !partitionByCol.isPresent();

    final KeyBuilder keyBuilder = StructKeyUtil.keyBuilder(resultSchema);

    final BiPredicate<Struct, GenericRow> predicate = (k, v) -> {
      if (v == null) {
        return false;
      }

      return evaluator.apply(v) != null;
    };

    final BiFunction<Struct, GenericRow, KeyValue<Struct, GenericRow>> mapper = (k, v) -> {
      final Object newKey = evaluator.apply(v);
      final Struct structKey = keyBuilder.build(newKey);

      if (appendNewKey) {
        v.append(newKey);
      }

      return new KeyValue<>(structKey, v);
    };

    return new PartitionByParams(resultSchema, predicate, mapper);
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

    return row -> {
      try {
        return expressionMetadata.evaluate(row);
      } catch (final Exception e) {
        final String errorMsg = "Error computing new key from expression "
            + expressionMetadata.getExpression()
            + " : "
            + e.getMessage();

        logger.error(RecordProcessingError.recordProcessingError(errorMsg, e, row));
        return null;
      }
    };
  }
}
