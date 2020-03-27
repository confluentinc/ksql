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

package io.confluent.ksql.execution.streams;

import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.codegen.ExpressionMetadata;
import io.confluent.ksql.execution.expression.tree.ColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.DereferenceExpression;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.util.StructKeyUtil;
import io.confluent.ksql.execution.util.StructKeyUtil.KeyBuilder;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.logging.processing.RecordProcessingError;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.ColumnNames;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.SchemaUtil;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.apache.kafka.connect.data.Struct;

final class GroupByParamsFactory {

  private static final String GROUP_BY_VALUE_SEPARATOR = "|+|";
  private static final Object EVAL_FAILED = new Object();

  private GroupByParamsFactory() {
  }

  public static LogicalSchema buildSchema(
      final LogicalSchema sourceSchema,
      final List<ExpressionMetadata> groupBys,
      final KsqlConfig ksqlConfig
  ) {
    return groupBys.size() == 1
        ? singleExpressionSchema(sourceSchema, groupBys.get(0), ksqlConfig)
        : multiExpressionSchema(sourceSchema, ksqlConfig);
  }

  public static GroupByParams build(
      final LogicalSchema sourceSchema,
      final List<ExpressionMetadata> groupBys,
      final ProcessingLogger logger,
      final KsqlConfig ksqlConfig
  ) {
    if (groupBys.isEmpty()) {
      throw new IllegalArgumentException("No GROUP BY groupBys");
    }

    final Function<GenericRow, Struct> mapper = groupBys.size() == 1
        ? new SingleExpressionGrouper(groupBys.get(0), logger)::apply
        : new MultiExpressionGrouper(groupBys, logger)::apply;

    final LogicalSchema schema = buildSchema(sourceSchema, groupBys, ksqlConfig);

    return new GroupByParams(schema, mapper);
  }

  private static LogicalSchema multiExpressionSchema(
      final LogicalSchema sourceSchema,
      final KsqlConfig ksqlConfig
  ) {
    return buildSchemaWithKeyType(sourceSchema, Optional.empty(), SqlTypes.STRING, ksqlConfig);
  }

  private static LogicalSchema singleExpressionSchema(
      final LogicalSchema sourceSchema,
      final ExpressionMetadata groupBy,
      final KsqlConfig ksqlConfig
  ) {
    final SqlType keyType = groupBy.getExpressionType();
    final Expression groupByExp = groupBy.getExpression();

    final Optional<ColumnName> singleColumnName;

    if (ksqlConfig.getBoolean(KsqlConfig.KSQL_ANY_KEY_NAME_ENABLED)) {
      if (groupByExp instanceof ColumnReferenceExp) {
        singleColumnName = Optional.of(((ColumnReferenceExp) groupByExp).getColumnName());
      } else if (groupByExp instanceof DereferenceExpression) {
        singleColumnName = Optional.of(ColumnNames.generatedStructFieldColumnName(groupByExp));
      } else {
        singleColumnName = Optional.empty();
      }
    } else {
      singleColumnName = Optional.empty();
    }

    return buildSchemaWithKeyType(sourceSchema, singleColumnName, keyType, ksqlConfig);
  }

  private static LogicalSchema buildSchemaWithKeyType(
      final LogicalSchema sourceSchema,
      final Optional<ColumnName> keyColName,
      final SqlType keyType,
      final KsqlConfig ksqlConfig
  ) {
    final ColumnName keyName = keyColName
        .orElseGet(() ->
            ksqlConfig.getBoolean(KsqlConfig.KSQL_ANY_KEY_NAME_ENABLED)
                ? ColumnNames.columnAliasGenerator(Stream.of(sourceSchema)).get()
                : SchemaUtil.ROWKEY_NAME
        );

    return LogicalSchema.builder()
        .withRowTime()
        .keyColumn(keyName, keyType)
        .valueColumns(sourceSchema.value())
        .build();
  }

  private static Object processColumn(
      final int index,
      final ExpressionMetadata exp,
      final GenericRow row,
      final ProcessingLogger logger
  ) {
    final Supplier<String> errorMsgSupplier = () ->
        "Error calculating group-by column with index " + index + "."
            + " The source row will be excluded from the table.";

    final Object result = exp.evaluate(row, EVAL_FAILED, logger, errorMsgSupplier);
    if (result == EVAL_FAILED) {
      return null;
    }

    if (result == null) {
      logger.error(RecordProcessingError.recordProcessingError(
          "Group-by column with index " + index + " resolved to null."
              + " The source row will be excluded from the table.",
          row
      ));
      return null;
    }

    return result;
  }

  private static final class SingleExpressionGrouper {

    private final KeyBuilder keyBuilder;
    private final ExpressionMetadata expression;
    private final ProcessingLogger logger;

    SingleExpressionGrouper(
        final ExpressionMetadata expression,
        final ProcessingLogger logger
    ) {
      this.expression = requireNonNull(expression, "expression");
      this.keyBuilder = StructKeyUtil
          .keyBuilder(SchemaUtil.ROWKEY_NAME, expression.getExpressionType());
      this.logger = Objects.requireNonNull(logger, "logger");
    }

    public Struct apply(final GenericRow row) {
      final Object key = processColumn(0, expression, row, logger);
      if (key == null) {
        return null;
      }
      return keyBuilder.build(key);
    }
  }

  private static final class MultiExpressionGrouper {

    private final KeyBuilder keyBuilder;
    private final ImmutableList<ExpressionMetadata> expressions;
    private final ProcessingLogger logger;

    MultiExpressionGrouper(
        final List<ExpressionMetadata> expressions,
        final ProcessingLogger logger
    ) {
      this.expressions = ImmutableList.copyOf(requireNonNull(expressions, "expressions"));
      this.keyBuilder = StructKeyUtil.keyBuilder(SchemaUtil.ROWKEY_NAME, SqlTypes.STRING);
      this.logger = Objects.requireNonNull(logger, "logger");

      if (expressions.isEmpty()) {
        throw new IllegalArgumentException("Empty group by");
      }
    }

    public Struct apply(final GenericRow row) {
      final StringBuilder key = new StringBuilder();
      for (int i = 0; i < expressions.size(); i++) {
        final Object result = processColumn(i, expressions.get(i), row, logger);
        if (result == null) {
          return null;
        }

        if (key.length() > 0) {
          key.append(GROUP_BY_VALUE_SEPARATOR);
        }

        key.append(result);
      }

      return keyBuilder.build(key.toString());
    }
  }
}
