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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.execution.codegen.helpers.TriFunction;
import io.confluent.ksql.execution.interpreter.TermEvaluationContext;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.util.Pair;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;

public class LambdaFunctionTerms {

  public static class LambdaFunction1Term implements Term {

    private final List<Pair<String, SqlType>> argNamesToTypes;
    private final Term body;

    public LambdaFunction1Term(final List<Pair<String, SqlType>> argNamesToTypes, final Term body) {
      this.argNamesToTypes = argNamesToTypes;
      this.body = body;

      Preconditions.checkState(argNamesToTypes.size() == 1);
    }

    @Override
    public Object getValue(final TermEvaluationContext context) {
      return (Function<Object, Object>) arg0 -> {
        final String name0 = argNamesToTypes.get(0).getLeft();
        context.pushVariableMappings(ImmutableMap.of(name0, arg0));
        try {
          return body.getValue(context);
        } finally {
          context.popVariableMappings();
        }
      };
    }

    @Override
    public SqlType getSqlType() {
      return null;
    }
  }

  public static class LambdaFunction2Term implements Term {

    private final List<Pair<String, SqlType>> argNamesToTypes;
    private final Term body;

    public LambdaFunction2Term(final List<Pair<String, SqlType>> argNamesToTypes, final Term body) {
      this.argNamesToTypes = argNamesToTypes;
      this.body = body;

      Preconditions.checkState(argNamesToTypes.size() == 2);
    }

    @Override
    public Object getValue(final TermEvaluationContext context) {
      return (BiFunction<Object, Object, Object>) (arg0, arg1) -> {
        final String name0 = argNamesToTypes.get(0).getLeft();
        final String name1 = argNamesToTypes.get(1).getLeft();
        context.pushVariableMappings(ImmutableMap.of(name0, arg0, name1, arg1));
        try {
          return body.getValue(context);
        } finally {
          context.popVariableMappings();
        }
      };
    }

    @Override
    public SqlType getSqlType() {
      return null;
    }
  }

  public static class LambdaFunction3Term implements Term {

    private final List<Pair<String, SqlType>> argNamesToTypes;
    private final Term body;

    public LambdaFunction3Term(final List<Pair<String, SqlType>> argNamesToTypes, final Term body) {
      this.argNamesToTypes = argNamesToTypes;
      this.body = body;

      Preconditions.checkState(argNamesToTypes.size() == 3);
    }

    @Override
    public Object getValue(final TermEvaluationContext context) {
      return (TriFunction<Object, Object, Object, Object>) (arg0, arg1, arg2) -> {
        final String name0 = argNamesToTypes.get(0).getLeft();
        final String name1 = argNamesToTypes.get(1).getLeft();
        final String name2 = argNamesToTypes.get(2).getLeft();
        context.pushVariableMappings(ImmutableMap.of(name0, arg0, name1, arg1, name2, arg2));
        try {
          return body.getValue(context);
        } finally {
          context.popVariableMappings();
        }
      };
    }

    @Override
    public SqlType getSqlType() {
      return null;
    }
  }
}
