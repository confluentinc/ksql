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

public class CastTerm implements Term {

  private final Term term;
  private final SqlType sqlType;
  private final CastFunction castFunction;

  public CastTerm(
      final Term term,
      final SqlType sqlType,
      final CastFunction castFunction
  ) {
    this.term = term;
    this.sqlType = sqlType;
    this.castFunction = castFunction;
  }

  @Override
  public Object getValue(final TermEvaluationContext context) {
    final Object value = term.getValue(context);
    if (value == null) {
      return null;
    }
    return castFunction.cast(value);
  }

  @Override
  public SqlType getSqlType() {
    return sqlType;
  }

  public interface CastFunction {
    Object cast(Object object);
  }

  public interface ComparableCastFunction<T extends Comparable<T>> extends CastFunction {
    T cast(Object object);
  }
}
