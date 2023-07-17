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

import io.confluent.ksql.execution.codegen.helpers.InListEvaluator;
import io.confluent.ksql.execution.interpreter.TermEvaluationContext;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import java.util.List;

public class InPredicateTerm implements Term {

  private final Term value;
  private final List<Term> valueList;

  public InPredicateTerm(final Term value, final List<Term> valueList) {
    this.value = value;
    this.valueList = valueList;
  }

  @Override
  public Object getValue(final TermEvaluationContext context) {
    final Object[] values = valueList.stream()
        .map(v -> v.getValue(context))
        .toArray();
    return InListEvaluator.matches(value.getValue(context), values);
  }

  @Override
  public SqlType getSqlType() {
    return SqlTypes.BOOLEAN;
  }
}
