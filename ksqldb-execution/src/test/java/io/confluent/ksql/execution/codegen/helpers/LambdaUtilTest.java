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

import io.confluent.ksql.execution.codegen.CodeGenTestUtil;
import java.util.function.Function;
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
        .function(argName, argType, argName + ".longValue() + 1");

    // Then:
    final Object result = CodeGenTestUtil.cookAndEval(javaCode, Function.class);
    assertThat(result, is(instanceOf(Function.class)));
    assertThat(((Function<Object, Object>) result).apply(10L), is(11L));
  }
}