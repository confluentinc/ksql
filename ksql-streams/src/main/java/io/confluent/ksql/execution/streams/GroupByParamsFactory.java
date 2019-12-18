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
import io.confluent.ksql.execution.util.StructKeyUtil;
import io.confluent.ksql.execution.util.StructKeyUtil.KeyBuilder;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.SchemaUtil;
import java.util.List;
import java.util.function.Function;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class GroupByParamsFactory {

  private static final Logger LOG = LoggerFactory.getLogger(GroupByParamsFactory.class);

  private static final String GROUP_BY_VALUE_SEPARATOR = "|+|";

  private GroupByParamsFactory() {
  }

  public static GroupByParams build(
      final LogicalSchema sourceSchema,
      final List<ExpressionMetadata> expressions
  ) {
    final Function<GenericRow, Struct> mapper = expressions.size() == 1
        ? new SingleExpressionGrouper(expressions.get(0))::apply
        : new MultiExpressionGrouper(expressions)::apply;

    final LogicalSchema schema = expressions.size() == 1
        ? singleExpressionSchema(sourceSchema, expressions.get(0).getExpressionType())
        : multiExpressionSchema(sourceSchema);

    return new GroupByParams(schema, mapper);
  }

  private static LogicalSchema multiExpressionSchema(
      final LogicalSchema sourceSchema
  ) {
    return buildSchema(sourceSchema, SqlTypes.STRING);
  }

  private static LogicalSchema singleExpressionSchema(
      final LogicalSchema sourceSchema,
      final SqlType rowKeyType
  ) {
    return buildSchema(sourceSchema, rowKeyType);
  }

  private static LogicalSchema buildSchema(
      final LogicalSchema sourceSchema,
      final SqlType rowKeyType
  ) {
    return LogicalSchema.builder()
        .keyColumn(SchemaUtil.ROWKEY_NAME, rowKeyType)
        .valueColumns(sourceSchema.value())
        .build();
  }

  private static Object processColumn(
      final int index,
      final ExpressionMetadata exp,
      final GenericRow row
  ) {
    try {
      final Object result = exp.evaluate(row);
      if (result == null) {
        LOG.error("Group-by column with index {} resolved to null. "
            + "The source row will be excluded from the table.", index);
      }
      return result;
    } catch (final Exception e) {
      LOG.error("Error calculating group-by column with index {}. "
          + "The source row will be excluded from the table.", index, e);
      return null;
    }
  }

  private static final class SingleExpressionGrouper {

    private final KeyBuilder keyBuilder;
    private final ExpressionMetadata expression;

    SingleExpressionGrouper(final ExpressionMetadata expression) {
      this.expression = requireNonNull(expression, "expression");
      this.keyBuilder = StructKeyUtil.keyBuilder(expression.getExpressionType());
    }

    public Struct apply(final GenericRow row) {
      final Object rowKey = processColumn(0, expression, row);
      if (rowKey == null) {
        return null;
      }
      return keyBuilder.build(rowKey);
    }
  }

  private static final class MultiExpressionGrouper {

    private final KeyBuilder keyBuilder;
    private final ImmutableList<ExpressionMetadata> expressions;

    MultiExpressionGrouper(final List<ExpressionMetadata> expressions) {
      this.expressions = ImmutableList.copyOf(requireNonNull(expressions, "expressions"));
      this.keyBuilder = StructKeyUtil.keyBuilder(SqlTypes.STRING);

      if (expressions.isEmpty()) {
        throw new IllegalArgumentException("Empty group by");
      }
    }

    public Struct apply(final GenericRow row) {
      final StringBuilder rowKey = new StringBuilder();
      for (int i = 0; i < expressions.size(); i++) {
        final Object result = processColumn(i, expressions.get(i), row);
        if (result == null) {
          return null;
        }

        if (rowKey.length() > 0) {
          rowKey.append(GROUP_BY_VALUE_SEPARATOR);
        }

        rowKey.append(result);
      }

      return keyBuilder.build(rowKey.toString());
    }
  }
}
