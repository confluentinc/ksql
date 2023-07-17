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

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.codegen.CodeGenRunner;
import io.confluent.ksql.execution.codegen.ExpressionMetadata;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.NullLiteral;
import io.confluent.ksql.execution.expression.tree.VisitParentExpressionVisitor;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.logging.processing.RecordProcessingError;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.DefaultSqlValueCoercer;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.SchemaConverters;
import io.confluent.ksql.schema.ksql.SqlValueCoercer;
import io.confluent.ksql.schema.ksql.types.SqlBaseType;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Builds a Java object, coerced to the desired type, from an arbitrary SQL
 * expression that does not reference any source data.
 */
class GenericExpressionResolver {

  private static final Supplier<String> IGNORED_MSG = () -> "";
  private static final ProcessingLogger THROWING_LOGGER = errorMessage -> {
    throw new KsqlException(((RecordProcessingError) errorMessage).getMessage());
  };

  private final SqlType fieldType;
  private final ColumnName fieldName;
  private final LogicalSchema schema;
  private final SqlValueCoercer sqlValueCoercer = DefaultSqlValueCoercer.INSTANCE;
  private final FunctionRegistry functionRegistry;
  private final KsqlConfig config;

  GenericExpressionResolver(
      final SqlType fieldType,
      final ColumnName fieldName,
      final LogicalSchema schema,
      final FunctionRegistry functionRegistry,
      final KsqlConfig config
  ) {
    this.fieldType = Objects.requireNonNull(fieldType, "fieldType");
    this.fieldName = Objects.requireNonNull(fieldName, "fieldName");
    this.schema = Objects.requireNonNull(schema, "schema");
    this.functionRegistry = Objects.requireNonNull(functionRegistry, "functionRegistry");
    this.config = Objects.requireNonNull(config, "config");
  }

  public Object resolve(final Expression expression) {
    return new Visitor().process(expression, null);
  }

  private class Visitor extends VisitParentExpressionVisitor<Object, Void> {

    @Override
    protected Object visitExpression(final Expression expression, final Void context) {
      final ExpressionMetadata metadata =
          CodeGenRunner.compileExpression(
              expression,
              "insert value",
              schema,
              config,
              functionRegistry
          );

      // we expect no column references, so we can pass in an empty generic row
      final Object value = metadata.evaluate(new GenericRow(), null, THROWING_LOGGER, IGNORED_MSG);

      return sqlValueCoercer.coerce(value, fieldType)
          .orElseThrow(() -> {
            final SqlBaseType valueSqlType = SchemaConverters.javaToSqlConverter()
                .toSqlType(value.getClass());

            return new KsqlException(
                String.format("Expected type %s for field %s but got %s(%s)",
                    fieldType,
                    fieldName,
                    valueSqlType,
                    value));
          })
          .orElse(null);
    }

    @Override
    public Object visitNullLiteral(final NullLiteral node, final Void context) {
      return null;
    }
  }

}
