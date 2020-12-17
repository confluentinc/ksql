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

import io.confluent.ksql.GenericKey;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.codegen.CodeGenRunner;
import io.confluent.ksql.execution.codegen.ExpressionMetadata;
import io.confluent.ksql.execution.expression.tree.ColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.NullLiteral;
import io.confluent.ksql.execution.plan.ExecutionKeyFactory;
import io.confluent.ksql.execution.streams.PartitionByParams.Mapper;
import io.confluent.ksql.execution.util.ColumnExtractor;
import io.confluent.ksql.execution.util.ExpressionTypeManager;
import io.confluent.ksql.execution.util.KeyUtil;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.ColumnAliasGenerator;
import io.confluent.ksql.schema.ksql.ColumnNames;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.LogicalSchema.Builder;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
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

  public static <K> PartitionByParams<K> build(
      final LogicalSchema sourceSchema,
      final ExecutionKeyFactory<K> serdeFactory,
      final List<Expression> partitionBy, // TODO: rename?
      final KsqlConfig ksqlConfig,
      final FunctionRegistry functionRegistry,
      final ProcessingLogger logger
  ) {
    final List<PartitionByColumn> partitionByCols =
        getPartitionByColumnName(sourceSchema, partitionBy);

    final LogicalSchema resultSchema =
        buildSchema(sourceSchema, partitionBy, functionRegistry, partitionByCols);

    final Mapper<K> mapper;
    if (isPartitionByNull(partitionBy)) {
      // In case of PARTITION BY NULL, it is sufficient to set the new key to null as the old key
      // is already present in the current value
      mapper = (k, v) -> new KeyValue<>(null, v);
    } else {
      final Set<? extends ColumnReferenceExp> sourceColsInPartitionBy = partitionBy.stream()
          .flatMap(pby -> ColumnExtractor.extractColumns(pby).stream())
          .collect(Collectors.toSet());
      final boolean partitionByInvolvesKeyColsOnly = sourceColsInPartitionBy.stream()
          .map(ColumnReferenceExp::getColumnName)
          .allMatch(sourceSchema::isKeyColumn);

      // TODO: what should be the behavior if some partition by expressions involve value columns but others don't, and a tombstone is received? should we drop the entire record? that's what's currently happening in this implementation. I think -- check
      final List<PartitionByExpressionEvaluator> evaluators = partitionBy.stream()
          .map(pby -> buildExpressionEvaluator(
              sourceSchema,
              pby,
              ksqlConfig,
              functionRegistry,
              logger,
              partitionByInvolvesKeyColsOnly
          )).collect(Collectors.toList());
      mapper = buildMapper(resultSchema, partitionByCols, evaluators, serdeFactory);
    }

    return new PartitionByParams<K>(resultSchema, mapper); // TODO: remove K
  }

  public static LogicalSchema buildSchema(
      final LogicalSchema sourceSchema,
      final List<Expression> partitionBy, // TODO: rename?
      final FunctionRegistry functionRegistry
  ) {
    final List<PartitionByColumn> partitionByCols =
        getPartitionByColumnName(sourceSchema, partitionBy);

    return buildSchema(sourceSchema, partitionBy, functionRegistry, partitionByCols);
  }

  // TODO: move
  public static boolean isPartitionByNull(final List<Expression> partitionBys) {
    final boolean nullExpressionPresent = partitionBys.stream()
        .anyMatch(pb -> pb instanceof NullLiteral);

    if (!nullExpressionPresent) {
      return false;
    }

    if (partitionBys.size() > 1) {
      throw new KsqlException("Cannot PARTITION BY multiple columns including NULL");
    }

    return true;
  }

  private static LogicalSchema buildSchema(
      final LogicalSchema sourceSchema,
      final List<Expression> partitionBy, // TODO: rename?
      final FunctionRegistry functionRegistry,
      final List<PartitionByColumn> partitionByCols
  ) {
    final ExpressionTypeManager expressionTypeManager =
        new ExpressionTypeManager(sourceSchema, functionRegistry);

    final List<SqlType> keyTypes = partitionBy.stream()
        .map(expressionTypeManager::getExpressionSqlType)
        .collect(Collectors.toList());

    if (isPartitionByNull(partitionBy)) {
      final Builder builder = LogicalSchema.builder();
      builder.valueColumns(sourceSchema.value());
      return builder.build();
    } else {
      final Builder builder = LogicalSchema.builder();
      for (int i = 0; i < partitionBy.size(); i++) {
        builder.keyColumn(partitionByCols.get(i).name, keyTypes.get(i));
      }

      builder.valueColumns(sourceSchema.value());
      for (int i = 0; i < partitionBy.size(); i++) {
        if (partitionByCols.get(i).shouldAppend) {
          // New key column added, copy in to value schema:
          builder.valueColumn(partitionByCols.get(i).name, keyTypes.get(i));
        }
      }

      return builder.build();
    }
  }

  // TODO: move, add accessors?
  private static class PartitionByColumn {
    final ColumnName name;
    final boolean shouldAppend;

    PartitionByColumn(final ColumnName name, final boolean shouldAppend) {
      this.name = Objects.requireNonNull(name, "name");
      this.shouldAppend = shouldAppend;
    }
  }

  private static List<PartitionByColumn> getPartitionByColumnName(
      final LogicalSchema sourceSchema,
      final List<Expression> partitionByExpressions
  ) {
    final ColumnAliasGenerator columnAliasGenerator =
        ColumnNames.columnAliasGenerator(Stream.of(sourceSchema));
    return partitionByExpressions.stream()
        .map(partitionBy -> {
          if (partitionBy instanceof ColumnReferenceExp) {
            // PARTITION BY column:
            final ColumnName columnName = ((ColumnReferenceExp) partitionBy).getColumnName();

            final Column column = sourceSchema
                .findValueColumn(columnName)
                .orElseThrow(() ->
                    new IllegalStateException("Unknown partition by column: " + columnName));

            return new PartitionByColumn(column.name(), false);
          } else {
            return new PartitionByColumn(columnAliasGenerator.uniqueAliasFor(partitionBy), true);
          }
        })
        .collect(Collectors.toList());
  }

  private static <K> Mapper<K> buildMapper(
      final LogicalSchema resultSchema, // TODO: remove dead param (and simplify build())
      final List<PartitionByColumn> partitionByCol,
      final List<PartitionByExpressionEvaluator> evaluators,
      final ExecutionKeyFactory<K> executionKeyFactory
  ) {
    return (oldK, row) -> {
      final List<Object> newKeyComponents = evaluators.stream()
          .map(evaluator -> evaluator.evaluate(oldK, row))
          .collect(Collectors.toList());

      final K key =
          executionKeyFactory.constructNewKey(oldK, GenericKey.fromList(newKeyComponents));

      if (row != null) {
        for (int i = 0; i < partitionByCol.size(); i++) {
          if (partitionByCol.get(i).shouldAppend) {
            // If partitioning by something other than an existing column, then a new key will have
            // been synthesized. This new key must be appended to the value to make it available for
            // stream processing, in the same way SourceBuilder appends the key and rowtime to the
            // value:
            row.append(newKeyComponents.get(i));
          }
        }
      }

      return new KeyValue<>(key, row);
    };
  }

  private static PartitionByExpressionEvaluator buildExpressionEvaluator(
      final LogicalSchema schema,
      final Expression partitionBy,
      final KsqlConfig ksqlConfig,
      final FunctionRegistry functionRegistry,
      final ProcessingLogger logger,
      final boolean partitionByInvolvesKeyColsOnly
  ) {
    final CodeGenRunner codeGen = new CodeGenRunner(
        partitionByInvolvesKeyColsOnly ? schema.withKeyColsOnly() : schema,
        ksqlConfig,
        functionRegistry
    );

    final ExpressionMetadata expressionMetadata = codeGen
        .buildCodeGenFromParseTree(partitionBy, "SelectKey");

    final String errorMsg = "Error computing new key from expression "
        + expressionMetadata.getExpression();

    return new PartitionByExpressionEvaluator(
        expressionMetadata,
        logger,
        () -> errorMsg,
        partitionByInvolvesKeyColsOnly
    );
  }

  private static class PartitionByExpressionEvaluator {

    private final ExpressionMetadata expressionMetadata;
    private final ProcessingLogger logger;
    private final Supplier<String> errorMsg;
    private final boolean evaluateOnKeyOnly;

    PartitionByExpressionEvaluator(
        final ExpressionMetadata expressionMetadata,
        final ProcessingLogger logger,
        final Supplier<String> errorMsg,
        final boolean evaluateOnKeyOnly
    ) {
      this.expressionMetadata = Objects.requireNonNull(expressionMetadata, "expressionMetadata");
      this.logger = Objects.requireNonNull(logger, "logger");
      this.errorMsg = Objects.requireNonNull(errorMsg, "errorMsg");
      this.evaluateOnKeyOnly = evaluateOnKeyOnly;
    }

    Object evaluate(final Object key, final GenericRow value) {
      final GenericRow row = evaluateOnKeyOnly
          ? GenericRow.fromList(KeyUtil.asList(key))
          : value;
      return expressionMetadata.evaluate(row, null, logger, errorMsg);
    }
  }
}
