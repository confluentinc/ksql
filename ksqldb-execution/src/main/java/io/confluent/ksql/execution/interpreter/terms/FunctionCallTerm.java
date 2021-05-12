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
import io.confluent.ksql.function.udf.Kudf;
import io.confluent.ksql.schema.ksql.types.SqlType;
import java.util.List;
import java.util.stream.Collectors;

public class FunctionCallTerm implements Term {

  private final Kudf kudf;
  private final List<Term> arguments;
  private final Class<?> resultJavaClass;
  private final SqlType resultType;

  public FunctionCallTerm(
      final Kudf kudf,
      final List<Term> arguments,
      final Class<?> resultJavaClass,
      final SqlType resultType
  ) {
    this.kudf = kudf;
    this.arguments = arguments;
    this.resultJavaClass = resultJavaClass;
    this.resultType = resultType;
  }

  @Override
  public Object getValue(final TermEvaluationContext context) {
    final List<Object> argObjects = arguments.stream()
        .map(term -> term.getValue(context))
        .collect(Collectors.toList());
    final Object result = kudf.evaluate(argObjects.toArray());
    return resultJavaClass.cast(result);
  }

  @Override
  public SqlType getSqlType() {
    return resultType;
  }
}
