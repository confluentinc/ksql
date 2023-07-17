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

public class SubscriptTerm implements Term {

  private final Term baseObjectTerm;
  private final Term subscriptTerm;
  private final SubscriptFunction subscriptFunction;
  private final SqlType sqlType;

  public SubscriptTerm(final Term baseObjectTerm, final Term subscriptTerm,
      final SubscriptFunction subscriptFunction, final SqlType sqlType) {
    this.baseObjectTerm = baseObjectTerm;
    this.subscriptTerm = subscriptTerm;
    this.subscriptFunction = subscriptFunction;
    this.sqlType = sqlType;
  }

  @Override
  public Object getValue(final TermEvaluationContext context) {
    return subscriptFunction.access(baseObjectTerm.getValue(context),
        subscriptTerm.getValue(context));
  }

  @Override
  public SqlType getSqlType() {
    return sqlType;
  }

  public interface SubscriptFunction {
    Object access(Object o, Object subscript);
  }
}
