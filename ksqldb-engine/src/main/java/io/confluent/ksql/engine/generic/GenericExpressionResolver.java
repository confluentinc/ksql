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

import static io.confluent.ksql.schema.ksql.types.SqlBaseType.STRING;
import static io.confluent.ksql.schema.ksql.types.SqlTypes.DATE;
import static io.confluent.ksql.schema.ksql.types.SqlTypes.TIME;
import static io.confluent.ksql.schema.ksql.types.SqlTypes.TIMESTAMP;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.codegen.CodeGenRunner;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.NullLiteral;
import io.confluent.ksql.execution.expression.tree.QualifiedColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.TraversalExpressionVisitor;
import io.confluent.ksql.execution.expression.tree.UnqualifiedColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.VisitParentExpressionVisitor;
import io.confluent.ksql.execution.interpreter.InterpretedExpressionFactory;
import io.confluent.ksql.execution.transform.ExpressionEvaluator;
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
public class GenericExpressionResolver {

  // GenericExpressionResolver doesn't accept any column references, so we don't
  // actually need the schema, but the CodeGenRunner expects on to be passed in
  private static final LogicalSchema NO_COLUMNS = LogicalSchema.builder().build();

  private static final Supplier<String> IGNORED_MSG = () -> "";
  private static final ProcessingLogger THROWING_LOGGER = new ProcessingLogger() {
    @Override
    public void error(final ErrorMessage errorMessage) {
      throw new KsqlException(((RecordProcessingError) errorMessage).getMessage());
    }

    @Override
    public void close() {
      // no-op
    }
  };

  private final SqlType fieldType;
  private final ColumnName fieldName;
  private final SqlValueCoercer sqlValueCoercer = DefaultSqlValueCoercer.STRICT;
  private final FunctionRegistry functionRegistry;
  private final KsqlConfig config;
  private final String operation;
  private final boolean shouldUseInterpreter;

  public GenericExpressionResolver(
      final SqlType fieldType,
      final ColumnName fieldName,
      final FunctionRegistry functionRegistry,
      final KsqlConfig config,
      final String operation,
      final boolean shouldUseInterpreter
  ) {
    this.fieldType = Objects.requireNonNull(fieldType, "fieldType");
    this.fieldName = Objects.requireNonNull(fieldName, "fieldName");
    this.functionRegistry = Objects.requireNonNull(functionRegistry, "functionRegistry");
    this.config = Objects.requireNonNull(config, "config");
    this.operation = Objects.requireNonNull(operation, "operation");
    this.shouldUseInterpreter = shouldUseInterpreter;
  }

  public Object resolve(final Expression expression) {
    return new Visitor().process(expression, null);
  }

  private class Visitor extends VisitParentExpressionVisitor<Object, Void> {

    @Override
    protected Object visitExpression(final Expression expression, final Void context) {
      new EnsureNoColReferences(expression).process(expression, context);
      final ExpressionEvaluator evaluator = shouldUseInterpreter
          ? InterpretedExpressionFactory.create(expression, NO_COLUMNS, functionRegistry, config)
          : CodeGenRunner.compileExpression(
              expression,
              operation,
              NO_COLUMNS,
              config,
              functionRegistry);

      // we expect no column references, so we can pass in an empty generic row
      final Object value = evaluator.evaluate(new GenericRow(), null, THROWING_LOGGER, IGNORED_MSG);

      return sqlValueCoercer.coerce(value, fieldType)
          .orElseThrow(() -> {
            final SqlBaseType valueSqlType = SchemaConverters.javaToSqlConverter()
                .toSqlType(value.getClass());
            final String errorMessage = String.format(
                "Expected type %s for field %s but got %s(%s)%s",
                fieldType,
                fieldName,
                valueSqlType,
                value,
                parseTimeErrorMessage(fieldType, valueSqlType)
            );
            return new KsqlException(errorMessage);
          })
          .orElse(null);
    }

    private String parseTimeErrorMessage(final SqlType fieldType, final SqlBaseType valueType) {
      if (valueType == STRING) {
        if (fieldType == TIMESTAMP) {
          return ". Timestamp format must be yyyy-mm-ddThh:mm:ss[.S]";
        } else if (fieldType == TIME) {
          return ". Time format must be hh:mm:ss[.S]";
        } else if (fieldType == DATE) {
          return ". Date format must be yyyy-mm-dd";
        }
      }
      return "";
    }

    @Override
    public Object visitNullLiteral(final NullLiteral node, final Void context) {
      return null;
    }
  }

  private class EnsureNoColReferences extends TraversalExpressionVisitor<Void> {

    private final Expression parent;

    EnsureNoColReferences(final Expression parent) {
      this.parent = Objects.requireNonNull(parent, "parent");
    }

    @Override
    public Void visitUnqualifiedColumnReference(
        final UnqualifiedColumnReferenceExp node,
        final Void context
    ) {
      throw new KsqlException("Unsupported column reference in " + operation + ": " + parent);
    }

    @Override
    public Void visitQualifiedColumnReference(
        final QualifiedColumnReferenceExp node,
        final Void context
    ) {
      throw new KsqlException("Unsupported column reference in " + operation + ": " + parent);
    }
  }

}
