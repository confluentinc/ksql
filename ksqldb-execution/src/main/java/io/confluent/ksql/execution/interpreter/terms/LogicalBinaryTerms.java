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

import io.confluent.ksql.execution.expression.tree.LogicalBinaryExpression;
import io.confluent.ksql.execution.interpreter.TermEvaluationContext;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.KsqlException;

public final class LogicalBinaryTerms {

  private LogicalBinaryTerms() { }

  public static class AndTerm implements Term {
    private final Term left;
    private final Term right;

    public AndTerm(final Term left, final Term right) {
      this.left = left;
      this.right = right;
    }

    @Override
    public Object getValue(final TermEvaluationContext context) {
      return (Boolean) left.getValue(context) && (Boolean) right.getValue(context);
    }

    @Override
    public SqlType getSqlType() {
      return SqlTypes.BOOLEAN;
    }
  }

  public static class OrTerm implements Term {
    private final Term left;
    private final Term right;

    public OrTerm(final Term left, final Term right) {
      this.left = left;
      this.right = right;
    }

    @Override
    public Object getValue(final TermEvaluationContext context) {
      return (Boolean) left.getValue(context) || (Boolean) right.getValue(context);
    }

    @Override
    public SqlType getSqlType() {
      return SqlTypes.BOOLEAN;
    }
  }

  public static Term create(
      final LogicalBinaryExpression.Type type,
      final Term left,
      final Term right
  ) {
    switch (type) {
      case OR:
        return new OrTerm(left, right);
      case AND:
        return new AndTerm(left, right);
      default:
        throw new KsqlException("Unknown type " + type);
    }
  }
}
