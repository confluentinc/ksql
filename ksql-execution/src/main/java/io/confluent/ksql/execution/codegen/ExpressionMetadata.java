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
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.testing.EffectivelyImmutable;
import io.confluent.ksql.util.KsqlException;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Objects;
import org.codehaus.commons.compiler.IExpressionEvaluator;

@Immutable
public class ExpressionMetadata {

  @EffectivelyImmutable
  private final IExpressionEvaluator expressionEvaluator;
  private final SqlType expressionType;
  private final ThreadLocal<Object[]> threadLocalParameters;
  private final Expression expression;
  private final CodeGenSpec spec;

  public ExpressionMetadata(
      IExpressionEvaluator expressionEvaluator,
      CodeGenSpec spec,
      SqlType expressionType,
      Expression expression
  ) {
    this.expressionEvaluator = Objects.requireNonNull(expressionEvaluator, "expressionEvaluator");
    this.expressionType = Objects.requireNonNull(expressionType, "expressionType");
    this.expression = Objects.requireNonNull(expression, "expression");
    this.spec = Objects.requireNonNull(spec, "spec");
    this.threadLocalParameters = ThreadLocal.withInitial(() -> new Object[spec.arguments().size()]);
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

  public Object evaluate(GenericRow row) {
    try {
      return expressionEvaluator.evaluate(getParameters(row));
    } catch (InvocationTargetException e) {
      throw new KsqlException(e.getCause().getMessage(), e.getCause());
    }
  }

  private Object[] getParameters(GenericRow row) {
    Object[] parameters = this.threadLocalParameters.get();
    spec.resolve(row, parameters);
    return parameters;
  }
}
