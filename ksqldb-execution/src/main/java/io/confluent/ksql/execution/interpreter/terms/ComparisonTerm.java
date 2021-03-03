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
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import java.util.Optional;

public class ComparisonTerm  {

  public static class CompareToTerm implements Term {

    private final Term left;
    private final Term right;
    private final ComparisonNullCheckFunction nullCheckFunction;
    private final ComparisonFunction comparisonFunction;
    private final ComparisonCheckFunction comparisonCheckFunction;

    public CompareToTerm(
        final Term left,
        final Term right,
        final ComparisonNullCheckFunction nullCheckFunction,
        final ComparisonFunction comparisonFunction,
        final ComparisonCheckFunction comparisonCheckFunction
    ) {
      this.left = left;
      this.right = right;
      this.nullCheckFunction = nullCheckFunction;
      this.comparisonFunction = comparisonFunction;
      this.comparisonCheckFunction = comparisonCheckFunction;
    }

    @Override
    public Object getValue(final TermEvaluationContext context) {
      final Object leftObject = left.getValue(context);
      final Object rightObject = right.getValue(context);
      final Optional<Boolean> nullCheck = nullCheckFunction.checkNull(leftObject, rightObject);
      if (nullCheck.isPresent()) {
        return nullCheck.get();
      }
      final int compareTo = comparisonFunction.compareTo(leftObject, rightObject);
      return comparisonCheckFunction.doCheck(compareTo);
    }

    @Override
    public SqlType getSqlType() {
      return SqlTypes.BOOLEAN;
    }
  }

  public static class EqualsTerm implements Term {

    private final Term left;
    private final Term right;
    private final ComparisonNullCheckFunction nullCheckFunction;
    private final EqualsFunction equalsFunction;
    private final EqualsCheckFunction equalsCheckFunction;

    public EqualsTerm(
        final Term left,
        final Term right,
        final ComparisonNullCheckFunction nullCheckFunction,
        final EqualsFunction equalsFunction,
        final EqualsCheckFunction equalsCheckFunction
    ) {
      this.left = left;
      this.right = right;
      this.nullCheckFunction = nullCheckFunction;
      this.equalsFunction = equalsFunction;
      this.equalsCheckFunction = equalsCheckFunction;
    }

    @Override
    public Object getValue(final TermEvaluationContext context) {
      final Object leftObject = left.getValue(context);
      final Object rightObject = right.getValue(context);
      final Optional<Boolean> nullCheck = nullCheckFunction.checkNull(leftObject, rightObject);
      if (nullCheck.isPresent()) {
        return nullCheck.get();
      }
      final boolean equals = equalsFunction.equals(leftObject, rightObject);
      return equalsCheckFunction.doCheck(equals);
    }

    @Override
    public SqlType getSqlType() {
      return SqlTypes.BOOLEAN;
    }
  }


  public interface ComparisonFunction {

    int compareTo(Object left, Object right);
  }

  public interface ComparisonCheckFunction {

    boolean doCheck(int compareTo);
  }

  public interface EqualsFunction {

    boolean equals(Object left, Object right);
  }

  public interface EqualsCheckFunction {
    boolean doCheck(boolean equals);
  }

  public interface ComparisonNullCheckFunction {

    Optional<Boolean> checkNull(Object left, Object right);
  }
}
