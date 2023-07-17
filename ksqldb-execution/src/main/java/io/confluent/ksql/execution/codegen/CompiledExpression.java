/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.execution.codegen;

import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.codegen.CodeGenSpec.ArgumentSpec;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.transform.ExpressionEvaluator;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.logging.processing.RecordProcessingError;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.testing.EffectivelyImmutable;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;
import org.codehaus.commons.compiler.IExpressionEvaluator;

@Immutable
public class CompiledExpression implements ExpressionEvaluator {

  @EffectivelyImmutable
  private final IExpressionEvaluator expressionEvaluator;
  private final SqlType expressionType;
  private final Expression expression;
  private final CodeGenSpec spec;

  public CompiledExpression(
      final IExpressionEvaluator expressionEvaluator,
      final CodeGenSpec spec,
      final SqlType expressionType,
      final Expression expression
  ) {
    this.expressionEvaluator = Objects.requireNonNull(expressionEvaluator, "expressionEvaluator");
    this.expressionType = Objects.requireNonNull(expressionType, "expressionType");
    this.expression = Objects.requireNonNull(expression, "expression");
    this.spec = Objects.requireNonNull(spec, "spec");
  }

  public List<ArgumentSpec> arguments() {
    return spec.arguments();
  }

  public SqlType getExpressionType() {
    return expressionType;
  }

  public Expression getExpression() {
    return expression;
  }

  /**
   * Evaluate the expression against the supplied {@code row}.
   *
   * <p>On error the supplied {@code logger} is called with the details of the error and the method
   * return {@code null}.
   *
   * @param row the row of data to evaluate the expression against.
   * @param defaultValue the value to return if an exception is thrown.
   * @param logger an optional logger to log errors to. If not supplied the method throws on error.
   * @param errorMsg called to get the text for the logged error.
   * @return the result of the evaluation.
   */
  public Object evaluate(
      final GenericRow row,
      final Object defaultValue,
      final ProcessingLogger logger,
      final Supplier<String> errorMsg
  ) {
    try {
      return expressionEvaluator.evaluate(new Object[]{
          spec.resolveArguments(row),
          defaultValue,
          logger,
          row
      });
    } catch (final Exception e) {
      final Throwable cause = e instanceof InvocationTargetException
          ? e.getCause()
          : e;

      logger.error(RecordProcessingError.recordProcessingError(errorMsg.get(), cause, row));
      return defaultValue;
    }
  }
}
