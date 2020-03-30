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
import com.google.common.collect.Iterables;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.codegen.ExpressionMetadata;
import io.confluent.ksql.execution.expression.tree.ColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.DereferenceExpression;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.util.StructKeyUtil;
import io.confluent.ksql.execution.util.StructKeyUtil.KeyBuilder;
import io.confluent.ksql.logging.processing.NoopProcessingLogContext;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.logging.processing.RecordProcessingError;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.ColumnNames;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.SchemaUtil;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
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
    final ProcessingLogger logger = NoopProcessingLogContext.NOOP_LOGGER;

    return groupBys.size() == 1
        ? new SingleExpressionGrouper(sourceSchema, groupBys.get(0), logger, ksqlConfig).getSchema()
        : new MultiExpressionGrouper(sourceSchema, groupBys, logger, ksqlConfig).getSchema();
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

    final Grouper grouper = groupBys.size() == 1
        ? new SingleExpressionGrouper(sourceSchema, groupBys.get(0), logger, ksqlConfig)
        : new MultiExpressionGrouper(sourceSchema, groupBys, logger, ksqlConfig);

    return new GroupByParams(grouper.getSchema(), grouper::apply);
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

  private static KeyBuilder keyBuilder(final LogicalSchema schema) {
    final Column keyCol = Iterables.getOnlyElement(schema.key());
    return StructKeyUtil.keyBuilder(keyCol.name(), keyCol.type());
  }

  private interface Grouper {

    LogicalSchema getSchema();

    Struct apply(GenericRow row);
  }

  private static final class SingleExpressionGrouper implements Grouper {

    private final LogicalSchema schema;
    private final KeyBuilder keyBuilder;
    private final ExpressionMetadata groupBy;
    private final ProcessingLogger logger;

    SingleExpressionGrouper(
        final LogicalSchema sourceSchema,
        final ExpressionMetadata groupBy,
        final ProcessingLogger logger,
        final KsqlConfig ksqlConfig
    ) {
      this.schema = singleExpressionSchema(sourceSchema, groupBy, ksqlConfig);
      this.groupBy = requireNonNull(groupBy, "groupBy");
      this.keyBuilder = keyBuilder(schema);
      this.logger = Objects.requireNonNull(logger, "logger");
    }

    @Override
    public LogicalSchema getSchema() {
      return schema;
    }

    @Override
    public Struct apply(final GenericRow row) {
      final Object key = processColumn(0, groupBy, row, logger);
      if (key == null) {
        return null;
      }
      return keyBuilder.build(key);
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
  }

  private static final class MultiExpressionGrouper implements Grouper {

    private final LogicalSchema schema;
    private final KeyBuilder keyBuilder;
    private final ImmutableList<ExpressionMetadata> groupBys;
    private final ProcessingLogger logger;

    MultiExpressionGrouper(
        final LogicalSchema sourceSchema,
        final List<ExpressionMetadata> groupBys,
        final ProcessingLogger logger,
        final KsqlConfig ksqlConfig
    ) {
      this.schema = multiExpressionSchema(sourceSchema, ksqlConfig);
      this.groupBys = ImmutableList.copyOf(requireNonNull(groupBys, "groupBys"));
      this.keyBuilder = keyBuilder(schema);
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
    public Struct apply(final GenericRow row) {
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

      return keyBuilder.build(key.toString());
    }
  }

  private static LogicalSchema multiExpressionSchema(
      final LogicalSchema sourceSchema,
      final KsqlConfig ksqlConfig
  ) {
    return buildSchemaWithKeyType(sourceSchema, Optional.empty(), SqlTypes.STRING, ksqlConfig);
  }
}
