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
import io.confluent.ksql.GenericKey;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.codegen.CompiledExpression;
import io.confluent.ksql.execution.expression.tree.ColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.logging.processing.NoopProcessingLogContext;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.logging.processing.RecordProcessingError;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.ColumnNames;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

final class GroupByParamsV1Factory {

  private static final String GROUP_BY_VALUE_SEPARATOR = "|+|";
  private static final Object EVAL_FAILED = new Object();

  private GroupByParamsV1Factory() {
  }

  public static LogicalSchema buildSchema(
      final LogicalSchema sourceSchema,
      final List<CompiledExpression> groupBys
  ) {
    final ProcessingLogger logger = NoopProcessingLogContext.NOOP_LOGGER;

    return buildGrouper(sourceSchema, groupBys, logger)
        .getSchema();
  }

  public static GroupByParams build(
      final LogicalSchema sourceSchema,
      final List<CompiledExpression> groupBys,
      final ProcessingLogger logger
  ) {
    if (groupBys.isEmpty()) {
      throw new IllegalArgumentException("No GROUP BY groupBys");
    }

    final Grouper grouper = buildGrouper(sourceSchema, groupBys, logger);

    return new GroupByParams(grouper.getSchema(), grouper::apply);
  }

  private static Grouper buildGrouper(
      final LogicalSchema sourceSchema,
      final List<CompiledExpression> groupBys,
      final ProcessingLogger logger
  ) {
    return groupBys.size() == 1
        ? new SingleExpressionGrouper(sourceSchema, groupBys.get(0), logger)
        : new MultiExpressionGrouper(sourceSchema, groupBys, logger);
  }

  private static LogicalSchema buildSchemaWithKeyType(
      final LogicalSchema sourceSchema,
      final ColumnName keyName,
      final SqlType keyType
  ) {
    return LogicalSchema.builder()
        .keyColumn(keyName, keyType)
        .valueColumns(sourceSchema.value())
        .build();
  }

  private static Object processColumn(
      final int index,
      final CompiledExpression exp,
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

  private interface Grouper {

    LogicalSchema getSchema();

    GenericKey apply(GenericRow row);
  }

  private static final class SingleExpressionGrouper implements Grouper {

    private final LogicalSchema schema;
    private final CompiledExpression groupBy;
    private final ProcessingLogger logger;

    SingleExpressionGrouper(
        final LogicalSchema sourceSchema,
        final CompiledExpression groupBy,
        final ProcessingLogger logger
    ) {
      this.schema = singleExpressionSchema(sourceSchema, groupBy);
      this.groupBy = requireNonNull(groupBy, "groupBy");
      this.logger = Objects.requireNonNull(logger, "logger");
    }

    @Override
    public LogicalSchema getSchema() {
      return schema;
    }

    @Override
    public GenericKey apply(final GenericRow row) {
      final Object key = processColumn(0, groupBy, row, logger);
      if (key == null) {
        return null;
      }

      return GenericKey.genericKey(key);
    }

    private static LogicalSchema singleExpressionSchema(
        final LogicalSchema sourceSchema,
        final CompiledExpression groupBy
    ) {
      final SqlType keyType = groupBy.getExpressionType();
      final Expression groupByExp = groupBy.getExpression();

      final ColumnName singleColumnName = groupByExp instanceof ColumnReferenceExp
          ? ((ColumnReferenceExp) groupByExp).getColumnName()
          : ColumnNames.uniqueAliasFor(groupByExp, sourceSchema);

      return buildSchemaWithKeyType(sourceSchema, singleColumnName, keyType);
    }
  }

  private static final class MultiExpressionGrouper implements Grouper {

    private final LogicalSchema schema;
    private final ImmutableList<CompiledExpression> groupBys;
    private final ProcessingLogger logger;

    MultiExpressionGrouper(
        final LogicalSchema sourceSchema,
        final List<CompiledExpression> groupBys,
        final ProcessingLogger logger
    ) {
      this.schema = multiExpressionSchema(sourceSchema);
      this.groupBys = ImmutableList.copyOf(requireNonNull(groupBys, "groupBys"));
      this.logger = Objects.requireNonNull(logger, "logger");

      if (this.groupBys.isEmpty()) {
        throw new IllegalArgumentException("Empty group by");
      }
    }

    @Override
    public LogicalSchema getSchema() {
      return schema;
    }

    @Override
    public GenericKey apply(final GenericRow row) {
      final StringBuilder key = new StringBuilder();
      for (int i = 0; i < groupBys.size(); i++) {
        final Object result = processColumn(i, groupBys.get(i), row, logger);
        if (result == null) {
          return null;
        }

        if (key.length() > 0) {
          key.append(GROUP_BY_VALUE_SEPARATOR);
        }

        key.append(result);
      }

      return GenericKey.genericKey(key.toString());
    }
  }

  private static LogicalSchema multiExpressionSchema(
      final LogicalSchema sourceSchema
  ) {
    final ColumnName keyName = ColumnNames.nextKsqlColAlias(sourceSchema);
    return buildSchemaWithKeyType(sourceSchema, keyName, SqlTypes.STRING);
  }
}