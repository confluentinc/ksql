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

package io.confluent.ksql.function;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.function.types.ParamTypes;
import io.confluent.ksql.function.udaf.Udaf;
import io.confluent.ksql.name.FunctionName;
import io.confluent.ksql.schema.ksql.SqlTypeParser;
import java.lang.reflect.Method;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class UdafTypesTest {

  private static final FunctionName FUNCTION_NAME = FunctionName.of("Bob");

  @Mock
  private SqlTypeParser typeParser;

  @Test
  public void shouldWorkForUdafsWithNoInitParams() {
    // When:
    final UdafTypes types = createTypes("udafWithNoInitParams");

    // Then:
    assertThat(types.getInputSchema(""), is(ImmutableList.of(
       new ParameterInfo("val", ParamTypes.INTEGER, "", false)
    )));
  }

  @Test
  public void shouldAppendInitParamsToInputSchema() {
    // When:
    final UdafTypes types = createTypes("udafWithStringInitParam", String.class);

    // Then:
    assertThat(types.getInputSchema(""), is(ImmutableList.of(
        new ParameterInfo("val", ParamTypes.INTEGER, "", false),
        new ParameterInfo("initParam", ParamTypes.STRING, "", false)
    )));
  }

  @SuppressWarnings({"unused", "MethodMayBeStatic"})
  private Udaf<Integer, Long, Double> udafWithNoInitParams() {
    return null;
  }

  @SuppressWarnings({"unused", "MethodMayBeStatic"})
  private Udaf<Integer, Long, Double> udafWithStringInitParam(final String initParam) {
    return null;
  }

  private UdafTypes createTypes(final String methodName, final Class<?>... parameterTypes) {
    try {
      final Method method = UdafTypesTest.class.getDeclaredMethod(methodName, parameterTypes);
      return new UdafTypes(method, FUNCTION_NAME, typeParser);
    } catch (NoSuchMethodException e) {
      throw new AssertionError("Invalid test", e);
    }
  }
}