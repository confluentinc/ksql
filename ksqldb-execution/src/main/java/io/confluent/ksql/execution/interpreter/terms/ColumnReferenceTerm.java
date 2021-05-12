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

public class ColumnReferenceTerm implements Term {

  private final int rowIndex;
  private final SqlType sqlType;

  public ColumnReferenceTerm(
      final int rowIndex,
      final SqlType sqlType
  ) {
    this.rowIndex = rowIndex;
    this.sqlType = sqlType;
  }

  @Override
  public Object getValue(final TermEvaluationContext context) {
    return context.getRow().get(rowIndex);
  }

  @Override
  public SqlType getSqlType() {
    return sqlType;
  }
}
