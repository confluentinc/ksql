/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql.execution.interpreter;

import com.google.common.annotations.VisibleForTesting;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.interpreter.TermCompiler.Context;
import io.confluent.ksql.execution.interpreter.terms.Term;
import io.confluent.ksql.execution.util.ExpressionTypeManager;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.KsqlStatementException;
import java.util.Objects;

public final class InterpretedExpressionFactory {

  private InterpretedExpressionFactory() {

  }

  public static InterpretedExpression create(
      final Expression expression,
      final LogicalSchema schema,
      final FunctionRegistry functionRegistry,
      final KsqlConfig ksqlConfig
  ) {
    return create(expression, schema, functionRegistry, ksqlConfig, new Context());
  }

  @VisibleForTesting
  public static InterpretedExpression create(
      final Expression expression,
      final LogicalSchema schema,
      final FunctionRegistry functionRegistry,
      final KsqlConfig ksqlConfig,
      final Context context
  ) {
    try {
      final ExpressionTypeManager expressionTypeManager
          = new ExpressionTypeManager(schema, functionRegistry);
      final SqlType returnType = expressionTypeManager.getExpressionSqlType(expression,
          context.getLambdaSqlTypeMapping());
      if (returnType == null) {
        // This should only happen if the only thing in the expression is a null literal.  This
        // should fail the type checking well before making it here, so shouldn't happen in
        // practice.
        throw new KsqlException("NULL expression not supported");
      }
      final Term term = new TermCompiler(
          functionRegistry, schema, ksqlConfig, expressionTypeManager)
          .process(expression, context);
      return new InterpretedExpression(expression, returnType, term);
    } catch (KsqlStatementException e) {
      throw new KsqlStatementException(
          "Invalid expression: " + e.getMessage(),
          "Invalid expression: " + e.getUnloggedMessage() + ". expression: "
              + expression + ", schema:" + schema,
          Objects.toString(expression),
          e
      );
    } catch (KsqlException e) {
      throw new KsqlStatementException(
          "Invalid expression: " + e.getMessage(),
          "Invalid expression: " + e.getMessage() + ". expression: "
              + expression + ", schema:" + schema,
          Objects.toString(expression),
          e
      );
    } catch (final Exception e) {
      throw new RuntimeException("Unexpected error generating code for expression: " + expression,
          e);
    }
  }
}
