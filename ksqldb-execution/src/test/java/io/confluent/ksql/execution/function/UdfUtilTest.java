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
import io.confluent.ksql.function.types.StructType;
import io.confluent.ksql.name.FunctionName;
import io.confluent.ksql.util.KsqlException;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.data.Struct;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.junit.Test;

public class UdfUtilTest {

  private static final FunctionName FUNCTION_NAME = FunctionName.of("Test");

  @Test
  public void shouldPassIfArgsAreCorrect() {
    final Object[] args = new Object[]{"TtestArg1", 10L};
    UdfUtil.ensureCorrectArgs(FUNCTION_NAME, args, String.class, Long.class);
  }

  @Test(expected = KsqlException.class)
  public void shouldFailIfTypeIsIncorrect() {
    final Object[] args = new Object[]{"TtestArg1", 10L};
    UdfUtil.ensureCorrectArgs(FUNCTION_NAME, args, String.class, Boolean.class);
  }

  @Test(expected = KsqlException.class)
  public void shouldFailIfArgCountIsTooFew() {
    final Object[] args = new Object[]{"TtestArg1", 10L};
    UdfUtil.ensureCorrectArgs(FUNCTION_NAME, args, String.class, Boolean.class, String.class);
  }

  @Test(expected = KsqlException.class)
  public void shouldFailIfArgCountIsTooMany() {
    final Object[] args = new Object[]{"TtestArg1", 10L};
    UdfUtil.ensureCorrectArgs(FUNCTION_NAME, args, String.class);
  }

  @Test
  public void shouldPassWithNullArgs() {
    final Object[] args = new Object[]{"TtestArg1", null};
    UdfUtil.ensureCorrectArgs(FUNCTION_NAME, args, String.class, Long.class);
  }

  @Test
  public void shouldHandleSubTypes() {
    final Object[] args = new Object[]{1.345, 55};
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
    final Type type = getClass().getDeclaredMethod("mapType", Map.class)
        .getGenericParameterTypes()[0];
    final ParamType schema = UdfUtil.getSchemaFromType(type);
    assertThat(schema, instanceOf(MapType.class));
    assertThat(((MapType) schema).value(), equalTo(ParamTypes.INTEGER));
  }

  @Test
  public void shouldGetArraySchemaFromListClass() throws NoSuchMethodException {
    final Type type = getClass().getDeclaredMethod("listType", List.class)
        .getGenericParameterTypes()[0];
    final ParamType schema = UdfUtil.getSchemaFromType(type);
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

  @Test
  public void shouldGetStringSchemaFromStructClass() {
    assertThat(
        UdfUtil.getSchemaFromType(Struct.class),
        equalTo(StructType.ANY_STRUCT)
    );
  }

  @Test(expected = KsqlException.class)
  public void shouldThrowExceptionIfClassDoesntMapToSchema() {
    UdfUtil.getSchemaFromType(System.class);
  }

  @Test
  public void shouldGetGenericSchemaFromType() throws NoSuchMethodException {
    // Given:
    final Type genericType = getClass().getMethod("genericType").getGenericReturnType();

    // When:
    final ParamType returnType = UdfUtil.getSchemaFromType(genericType);

    // Then:
    MatcherAssert.assertThat(returnType, CoreMatchers.is(GenericType.of("T")));
  }

  @Test
  public void shouldGetGenericSchemaFromParameterizedType() throws NoSuchMethodException {
    // Given:
    final Type genericType = getClass().getMethod("genericMapType").getGenericReturnType();

    // When:
    final ParamType returnType = UdfUtil.getSchemaFromType(genericType);

    // Then:
    assertThat(returnType, is(MapType.of(GenericType.of("K"), GenericType.of("V"))));
  }

  @Test
  public void shouldGetGenericSchemaFromPartialParameterizedType() throws NoSuchMethodException {
    // Given:
    final Type genericType = getClass().getMethod("partialGenericMapType").getGenericReturnType();

    // When:
    final ParamType returnType = UdfUtil.getSchemaFromType(genericType);

    // Then:
    assertThat(returnType, is(MapType.of(ParamTypes.LONG, GenericType.of("V"))));
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
  public <K, V> Map<K, V> genericMapType() {
    return null;
  }

  @SuppressWarnings({"unused", "WeakerAccess", "MethodMayBeStatic"})
  public <V> Map<Long, V> partialGenericMapType() {
    return null;
  }

  @SuppressWarnings("unused")
  private void mapType(final Map<Long, Integer> map) {
  }

  @SuppressWarnings("unused")
  private void listType(final List<Double> list) {
  }
}
