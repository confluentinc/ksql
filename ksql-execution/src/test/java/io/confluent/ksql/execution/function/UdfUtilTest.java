/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.execution.function;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import io.confluent.ksql.function.types.ArrayType;
import io.confluent.ksql.function.types.GenericType;
import io.confluent.ksql.function.types.MapType;
import io.confluent.ksql.function.types.ParamType;
import io.confluent.ksql.function.types.ParamTypes;
import io.confluent.ksql.name.FunctionName;
import io.confluent.ksql.util.KsqlException;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.junit.Test;

public class UdfUtilTest {

  private static final FunctionName FUNCTION_NAME = FunctionName.of("Test");

  @Test
  public void shouldPassIfArgsAreCorrect() {
    Object[] args = new Object[]{"TtestArg1", 10L};
    UdfUtil.ensureCorrectArgs(FUNCTION_NAME, args, String.class, Long.class);
  }

  @Test(expected = KsqlException.class)
  public void shouldFailIfTypeIsIncorrect() {
    Object[] args = new Object[]{"TtestArg1", 10L};
    UdfUtil.ensureCorrectArgs(FUNCTION_NAME, args, String.class, Boolean.class);
  }

  @Test(expected = KsqlException.class)
  public void shouldFailIfArgCountIsTooFew() {
    Object[] args = new Object[]{"TtestArg1", 10L};
    UdfUtil.ensureCorrectArgs(FUNCTION_NAME, args, String.class, Boolean.class, String.class);
  }

  @Test(expected = KsqlException.class)
  public void shouldFailIfArgCountIsTooMany() {
    Object[] args = new Object[]{"TtestArg1", 10L};
    UdfUtil.ensureCorrectArgs(FUNCTION_NAME, args, String.class);
  }

  @Test
  public void shouldPassWithNullArgs() {
    Object[] args = new Object[]{"TtestArg1", null};
    UdfUtil.ensureCorrectArgs(FUNCTION_NAME, args, String.class, Long.class);
  }

  @Test
  public void shouldHandleSubTypes() {
    Object[] args = new Object[]{1.345, 55};
    UdfUtil.ensureCorrectArgs(FUNCTION_NAME, args, Number.class, Number.class);
  }

  @Test
  public void shouldGetBooleanSchemaForBooleanClass() {
    assertThat(
        UdfUtil.getSchemaFromType(Boolean.class),
        equalTo(ParamTypes.BOOLEAN)
    );
  }

  @Test
  public void shouldGetBooleanSchemaForBooleanPrimitiveClass() {
    assertThat(
        UdfUtil.getSchemaFromType(boolean.class),
        equalTo(ParamTypes.BOOLEAN)
    );
  }

  @Test
  public void shouldGetIntSchemaForIntegerClass() {
    assertThat(
        UdfUtil.getSchemaFromType(Integer.class),
        equalTo(ParamTypes.INTEGER)
    );
  }

  @Test
  public void shouldGetIntegerSchemaForIntPrimitiveClass() {
    assertThat(
        UdfUtil.getSchemaFromType(int.class),
        equalTo(ParamTypes.INTEGER)
    );
  }

  @Test
  public void shouldGetLongSchemaForLongClass() {
    assertThat(
        UdfUtil.getSchemaFromType(Long.class),
        equalTo(ParamTypes.LONG)
    );
  }

  @Test
  public void shouldGetLongSchemaForLongPrimitiveClass() {
    assertThat(
        UdfUtil.getSchemaFromType(long.class),
        equalTo(ParamTypes.LONG)
    );
  }

  @Test
  public void shouldGetFloatSchemaForDoubleClass() {
    assertThat(
        UdfUtil.getSchemaFromType(Double.class),
        equalTo(ParamTypes.DOUBLE)
    );
  }

  @Test
  public void shouldGetFloatSchemaForDoublePrimitiveClass() {
    assertThat(
        UdfUtil.getSchemaFromType(double.class),
        equalTo(ParamTypes.DOUBLE)
    );
  }

  @Test
  public void shouldGetDecimalSchemaForBigDecimalClass() {
    assertThat(
        UdfUtil.getSchemaFromType(BigDecimal.class),
        is(ParamTypes.DECIMAL)
    );
  }

  @Test
  public void shouldGetMapSchemaFromMapClass() throws NoSuchMethodException {
    Type type = getClass().getDeclaredMethod("mapType", Map.class)
        .getGenericParameterTypes()[0];
    ParamType schema = UdfUtil.getSchemaFromType(type);
    assertThat(schema, instanceOf(MapType.class));
    assertThat(((MapType) schema).value(), equalTo(ParamTypes.INTEGER));
  }

  @Test
  public void shouldGetArraySchemaFromListClass() throws NoSuchMethodException {
    Type type = getClass().getDeclaredMethod("listType", List.class)
        .getGenericParameterTypes()[0];
    ParamType schema = UdfUtil.getSchemaFromType(type);
    assertThat(schema, instanceOf(ArrayType.class));
    assertThat(((ArrayType) schema).element(), equalTo(ParamTypes.DOUBLE));
  }

  @Test
  public void shouldGetStringSchemaFromStringClass() {
    assertThat(
        UdfUtil.getSchemaFromType(String.class),
        equalTo(ParamTypes.STRING)
    );
  }

  @Test(expected = KsqlException.class)
  public void shouldThrowExceptionIfClassDoesntMapToSchema() {
    UdfUtil.getSchemaFromType(System.class);
  }

  @Test
  public void shouldGetGenericSchemaFromType() throws NoSuchMethodException {
    // Given:
    Type genericType = getClass().getMethod("genericType").getGenericReturnType();

    // When:
    ParamType returnType = UdfUtil.getSchemaFromType(genericType);

    // Then:
    MatcherAssert.assertThat(returnType, CoreMatchers.is(GenericType.of("T")));
  }

  @Test
  public void shouldGetGenericSchemaFromParameterizedType() throws NoSuchMethodException {
    // Given:
    Type genericType = getClass().getMethod("genericMapType").getGenericReturnType();

    // When:
    ParamType returnType = UdfUtil.getSchemaFromType(genericType);

    // Then:
    assertThat(returnType, is(MapType.of(GenericType.of("T"))));
  }

  // following methods not invoked but used to test conversion from type -> schema
  @SuppressWarnings({"unused", "WeakerAccess", "MethodMayBeStatic"})
  public <T> T genericType() {
    return null;
  }

  @SuppressWarnings({"unused", "MethodMayBeStatic"})
  public <T> List<T> genericArrayType() {
    return null;
  }

  @SuppressWarnings({"unused", "WeakerAccess", "MethodMayBeStatic"})
  public <T> Map<String, T> genericMapType() {
    return null;
  }

  @SuppressWarnings("unused")
  private void mapType(Map<String, Integer> map) {
  }

  @SuppressWarnings("unused")
  private void listType(List<Double> list) {
  }
}
