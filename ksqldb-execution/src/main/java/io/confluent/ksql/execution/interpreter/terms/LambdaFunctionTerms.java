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
import com.google.common.collect.ImmutableList;
import io.confluent.ksql.execution.codegen.helpers.TriFunction;
import io.confluent.ksql.execution.interpreter.TermEvaluationContext;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.util.Pair;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

public class LambdaFunctionTerms {

  public abstract static class LambdaFunctionBaseTerm implements Term {
    protected final List<Pair<String, SqlType>> argNamesToTypes;
    protected final Term body;

    public LambdaFunctionBaseTerm(
        final List<Pair<String, SqlType>> argNamesToTypes,
        final Term body
    ) {
      this.argNamesToTypes = ImmutableList.copyOf(argNamesToTypes);
      this.body = body;
    }

    @Override
    public SqlType getSqlType() {
      // A lambda isn't considered to have a type in our type system, so we return null.
      return null;
    }

    protected Object getValueCommon(final TermEvaluationContext context, final Object... args) {
      context.pushVariableMappings(createVariableMap(argNamesToTypes, args));
      try {
        return body.getValue(context);
      } finally {
        context.popVariableMappings();
      }
    }
  }

  public static class LambdaFunction1Term extends LambdaFunctionBaseTerm {

    public LambdaFunction1Term(final List<Pair<String, SqlType>> argNamesToTypes, final Term body) {
      super(argNamesToTypes, body);
      Preconditions.checkState(argNamesToTypes.size() == 1);
    }

    @Override
    public Object getValue(final TermEvaluationContext context) {
      return (Function<Object, Object>) arg0 -> getValueCommon(context, arg0);
    }
  }

  public static class LambdaFunction2Term extends LambdaFunctionBaseTerm {

    public LambdaFunction2Term(final List<Pair<String, SqlType>> argNamesToTypes, final Term body) {
      super(argNamesToTypes, body);
      Preconditions.checkState(argNamesToTypes.size() == 2);
    }

    @Override
    public Object getValue(final TermEvaluationContext context) {
      return (BiFunction<Object, Object, Object>) (arg0, arg1) ->
          getValueCommon(context, arg0, arg1);
    }
  }

  public static class LambdaFunction3Term extends LambdaFunctionBaseTerm {

    public LambdaFunction3Term(final List<Pair<String, SqlType>> argNamesToTypes, final Term body) {
      super(argNamesToTypes, body);
      Preconditions.checkState(argNamesToTypes.size() == 3);
    }

    @Override
    public Object getValue(final TermEvaluationContext context) {
      return (TriFunction<Object, Object, Object, Object>) (arg0, arg1, arg3) ->
          getValueCommon(context, arg0, arg1, arg3);
    }
  }

  private static Map<String, Object> createVariableMap(
      final List<Pair<String, SqlType>> argNamesToTypes,
      final Object... args
  ) {
    Preconditions.checkArgument(argNamesToTypes.size() == args.length,
        "Argument length should be the same");
    final Map<String, Object> variableMap = new HashMap<>();
    int i = 0;
    for (final Pair<String, SqlType> pair : argNamesToTypes) {
      final String name = pair.getLeft();
      variableMap.put(name, args[i]);
      i++;
    }
    return Collections.unmodifiableMap(variableMap);
  }
}
