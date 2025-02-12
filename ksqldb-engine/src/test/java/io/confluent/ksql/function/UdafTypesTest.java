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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.function.types.ParamTypes;
import io.confluent.ksql.function.udaf.Udaf;
import io.confluent.ksql.function.udaf.VariadicArgs;
import io.confluent.ksql.name.FunctionName;
import io.confluent.ksql.schema.ksql.SqlTypeParser;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.Pair;
import java.lang.reflect.Method;
import java.util.Collections;
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
    assertThat(types.getInputSchema(new String[]{""}), is(ImmutableList.of(
       new ParameterInfo("val1", ParamTypes.INTEGER, "", false)
    )));
    assertFalse(types.isVariadic());
    assertTrue(types.literalParams().isEmpty());
  }

  @Test
  public void shouldAppendInitParamsToInputSchema() {
    // When:
    final UdafTypes types = createTypes("udafWithStringInitParam", String.class);
    final ParameterInfo initParamInfo = new ParameterInfo(
            "initParam",
            ParamTypes.STRING,
            "",
            false
    );

    // Then:
    assertThat(types.getInputSchema(new String[]{""}), is(ImmutableList.of(
        new ParameterInfo("val1", ParamTypes.INTEGER, "", false),
        initParamInfo
    )));
    assertFalse(types.isVariadic());
    assertEquals(Collections.singletonList(initParamInfo), types.literalParams());
  }

  @Test
  public void shouldDefaultToEmptySchemasWhenNotEnoughProvided() {
    // When:
    final UdafTypes types = createTypes("udafWithMultipleParams", String.class);
    final ParameterInfo initParamInfo = new ParameterInfo(
            "initParam",
            ParamTypes.STRING,
            "",
            false
    );

    // Then:
    assertThat(types.getInputSchema(new String[]{""}), is(ImmutableList.of(
            new ParameterInfo("val1", ParamTypes.INTEGER, "", false),
            new ParameterInfo("val2", ParamTypes.DOUBLE, "", false),
            initParamInfo
    )));
    assertFalse(types.isVariadic());
    assertEquals(Collections.singletonList(initParamInfo), types.literalParams());
  }

  @Test
  public void shouldIgnoreExtraSchemasWhenTooManyProvided() {
    // When:
    final UdafTypes types = createTypes("udafWithMultipleParams", String.class);
    final ParameterInfo initParamInfo = new ParameterInfo(
            "initParam",
            ParamTypes.STRING,
            "",
            false
    );

    // Then:
    assertThat(types.getInputSchema(
            new String[]{"", "", "STRUCT<A INTEGER, B INTEGER>"}),
            is(ImmutableList.of(
              new ParameterInfo("val1", ParamTypes.INTEGER, "", false),
              new ParameterInfo("val2", ParamTypes.DOUBLE, "", false),
              initParamInfo
            ))
    );
    assertFalse(types.isVariadic());
    assertEquals(Collections.singletonList(initParamInfo), types.literalParams());
  }

  @Test
  public void shouldThrowWhenColArgAndInitArgAreVariadic() {
    final Exception e = assertThrows(
            KsqlException.class,
            () -> createTypes("udafWithVariadicColAndInitArgs", int[].class)
    );

    assertThat(e.getMessage(), is("A UDAF and its factory can have at most one variadic argument"));
  }

  // TEMPORARY: until variadic args in the middle are added
  @Test
  public void shouldThrowWhenOnlyColArgIsVariadic() {
    final Exception e = assertThrows(
            KsqlException.class,
            () -> createTypes("udafWithInitArgAndVariadicColArg", int.class)
    );

    assertThat(e.getMessage(), is("Methods with variadic column args cannot have factory args"));
  }

  @Test
  public void shouldNotThrowWhenOnlyInitArgIsVariadic() {
    UdafTypes udafTypes = createTypes("udafWithVariadicInitArg", int[].class);
    assertTrue(udafTypes.isVariadic());
    assertTrue(udafTypes.literalParams().get(0).isVariadic());
  }

  @Test
  public void shouldNotThrowWhenColArgIsVariadicNoInitArg() {
    UdafTypes udafTypes = createTypes("udafWithVariadicColArg");
    assertTrue(udafTypes.isVariadic());
    assertTrue(udafTypes.literalParams().isEmpty());
  }

  @Test
  public void shouldThrowWhenMultipleColsVariadic() {
    final Exception e = assertThrows(
            KsqlException.class,
            () -> createTypes("udafWithMultipleVariadicColArgs")
    );

    assertThat(e.getMessage(), is("A UDAF and its factory can have at most one variadic argument"));
  }

  @Test
  public void shouldThrowWhenColArgVariadicWithoutTuple() {
    final Exception e = assertThrows(
            KsqlException.class,
            () -> createTypes("udafWithVariadicColArgWithoutTuple")
    );

    assertThat(e.getMessage(), is("Variadic column arguments are only allowed inside tuples"));
  }

  @SuppressWarnings({"unused", "MethodMayBeStatic"})
  private Udaf<Pair<Integer, Double>, Long, Double> udafWithMultipleParams(String initParam) {
    return null;
  }

  @SuppressWarnings({"unused", "MethodMayBeStatic"})
  private Udaf<Integer, Long, Double> udafWithNoInitParams() {
    return null;
  }

  @SuppressWarnings({"unused", "MethodMayBeStatic"})
  private Udaf<Integer, Long, Double> udafWithStringInitParam(final String initParam) {
    return null;
  }

  @SuppressWarnings({"unused", "MethodMayBeStatic"})
  private Udaf<Pair<Integer, VariadicArgs<Double>>, Long, Double> udafWithVariadicColAndInitArgs(
          int... args) {
    return null;
  }

  @SuppressWarnings({"unused", "MethodMayBeStatic"})
  private Udaf<Pair<Integer, VariadicArgs<Double>>, Long, Double>
      udafWithInitArgAndVariadicColArg(int arg) {
    return null;
  }

  @SuppressWarnings({"unused", "MethodMayBeStatic"})
  private Udaf<Pair<Integer, Double>, Long, Double> udafWithVariadicInitArg(
          int... args) {
    return null;
  }

  @SuppressWarnings({"unused", "MethodMayBeStatic"})
  private Udaf<Pair<Integer, VariadicArgs<Double>>, Long, Double> udafWithVariadicColArg() {
    return null;
  }

  @SuppressWarnings({"unused", "MethodMayBeStatic"})
  private Udaf<Pair<VariadicArgs<Integer>, VariadicArgs<Double>>, Long, Double>
      udafWithMultipleVariadicColArgs() {
    return null;
  }

  @SuppressWarnings({"unused", "MethodMayBeStatic"})
  private Udaf<VariadicArgs<Integer>, Long, Double> udafWithVariadicColArgWithoutTuple() {
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