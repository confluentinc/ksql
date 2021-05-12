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
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.interpreter.terms.Term;
import io.confluent.ksql.execution.transform.ExpressionEvaluator;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.logging.processing.RecordProcessingError;
import io.confluent.ksql.schema.ksql.types.SqlType;
import java.util.function.Supplier;

public class InterpretedExpression implements ExpressionEvaluator {

  private final Term term;
  private final Expression expression;
  private final SqlType expressionType;

  public InterpretedExpression(
      final Expression expression,
      final SqlType expressionType,
      final Term term) {
    this.expression = expression;
    this.expressionType = expressionType;
    this.term = term;
  }

  @VisibleForTesting
  Object evaluate(final GenericRow row) {
    return term.getValue(new TermEvaluationContext(row));
  }

  @Override
  public Object evaluate(
      final GenericRow row,
      final Object defaultValue,
      final ProcessingLogger logger,
      final Supplier<String> errorMsg
  ) {
    try {
      return evaluate(row);
    } catch (final Exception e) {
      logger.error(RecordProcessingError.recordProcessingError(errorMsg.get(), e, row));
      return defaultValue;
    }
  }

  @Override
  public Expression getExpression() {
    return expression;
  }

  public SqlType getExpressionType() {
    return expressionType;
  }
}
