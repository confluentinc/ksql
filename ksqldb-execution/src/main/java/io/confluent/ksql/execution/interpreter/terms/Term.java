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

/**
 * The "compiled" state for an {@link io.confluent.ksql.execution.interpreter.InterpretedExpression}
 * that contains all information about the underlying expression necessary to evaluate it, without
 * knowing the input row(s) on which it will be invoked. Terms are meant to be reused across many
 * row evaluations and should therefore hold no row-based state. They are created directly from
 * {@link io.confluent.ksql.execution.interpreter.TermCompiler} and involve no java code generation,
 * hence being interpreted.
 */
public interface Term {

  /**
   * The main evaluation method which fetches or computes the value of the expression the term
   * represents.
   * @param termEvaluationContext The context for evaluating the term
   * @return The object which represents the evaluation of the underlying expression
   */
  Object getValue(TermEvaluationContext termEvaluationContext);

  /**
   * The compile time {@link SqlType} of this term.
   * @return
   */
  SqlType getSqlType();
}
