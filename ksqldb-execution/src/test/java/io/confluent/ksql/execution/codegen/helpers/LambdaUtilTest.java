/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.execution.codegen.helpers;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.execution.codegen.CodeGenTestUtil;

import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;

import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.Pair;
import org.junit.Test;

@SuppressWarnings("unchecked")
public class LambdaUtilTest {

  @Test
  public void shouldGenerateFunctionCode() {
    // Given:
    final String argName = "fred";
    final Class<?> argType = Long.class;

    // When:
    final String javaCode = LambdaUtil
        .toJavaCode(argName, argType, argName + " + 1");

    // Then:
    final Object result = CodeGenTestUtil.cookAndEval(javaCode, Function.class);
    assertThat(result, is(instanceOf(Function.class)));
    assertThat(((Function<Object, Object>) result).apply(10L), is(11L));
  }

  @Test
  public void shouldGenerateBiFunction() {
    // Given:
    final Pair<String, Class<?>> argName1 = new Pair<>("fred", Long.class);
    final Pair<String, Class<?>> argName2 = new Pair<>("bob", Long.class);
    
    final List<Pair<String, Class<?>>> argList = ImmutableList.of(argName1, argName2);

    // When:
    final String javaCode = LambdaUtil.toJavaCode(argList, "fred + bob + 2");

    // Then:
    final Object result = CodeGenTestUtil.cookAndEval(javaCode, BiFunction.class);
    assertThat(result, is(instanceOf(BiFunction.class)));
    assertThat(((BiFunction<Object, Object, Object>) result).apply(10L, 15L), is(27L));
  }

  @Test
  public void shouldGenerateTriFunction() {
    // Given:
    final Pair<String, Class<?>> argName1 = new Pair<>("fred", Long.class);
    final Pair<String, Class<?>> argName2 = new Pair<>("bob", Long.class);
    final Pair<String, Class<?>> argName3 = new Pair<>("tim", Long.class);

    final List<Pair<String, Class<?>>> argList = ImmutableList.of(argName1, argName2, argName3);

    // When:
    final String javaCode = LambdaUtil.toJavaCode(argList, "fred + bob + tim + 1");

    // Then:
    final Object result = CodeGenTestUtil.cookAndEval(javaCode, TriFunction.class);
    assertThat(result, is(instanceOf(TriFunction.class)));
    assertThat(((TriFunction<Object, Object, Object, Object>) result).apply(10L, 15L, 3L), is(29L));
  }

  @Test(expected= KsqlException.class)
  public void shouldThrowOnNonSupportedArguments() {
    // Given:
    final Pair<String, Class<?>> argName1 = new Pair<>("fred", Long.class);
    final Pair<String, Class<?>> argName2 = new Pair<>("bob", Long.class);
    final Pair<String, Class<?>> argName3 = new Pair<>("tim", Long.class);
    final Pair<String, Class<?>> argName4 = new Pair<>("hello", Long.class);

    final List<Pair<String, Class<?>>> argList = ImmutableList.of(argName1, argName2, argName3, argName4);

    // When:
    LambdaUtil.toJavaCode(argList, "fred + bob + tim + hello + 1");
  }
}