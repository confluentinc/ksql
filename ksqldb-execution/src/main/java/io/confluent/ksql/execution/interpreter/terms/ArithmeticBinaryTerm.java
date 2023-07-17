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

package io.confluent.ksql.execution.interpreter.terms;

import io.confluent.ksql.execution.interpreter.TermEvaluationContext;
import io.confluent.ksql.schema.ksql.types.SqlType;

public class ArithmeticBinaryTerm implements Term {

  private final Term left;
  private final Term right;
  private final ArithmeticBinaryFunction arithmeticBinaryFunction;
  private final SqlType resultType;

  public ArithmeticBinaryTerm(
      final Term left,
      final Term right,
      final ArithmeticBinaryFunction arithmeticBinaryFunction,
      final SqlType resultType
  ) {
    this.left = left;
    this.right = right;
    this.arithmeticBinaryFunction = arithmeticBinaryFunction;
    this.resultType = resultType;
  }

  @Override
  public Object getValue(final TermEvaluationContext context) {
    final Object leftObject = left.getValue(context);
    final Object rightObject = right.getValue(context);
    if (leftObject == null || rightObject == null) {
      return null;
    }
    return arithmeticBinaryFunction.doFunction(left.getValue(context), right.getValue(context));
  }

  @Override
  public SqlType getSqlType() {
    return resultType;
  }

  public interface ArithmeticBinaryFunction {
    Object doFunction(Object o1, Object o2);
  }
}
