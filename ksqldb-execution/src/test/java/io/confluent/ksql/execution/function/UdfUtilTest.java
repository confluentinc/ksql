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

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.execution.codegen.helpers.TriFunction;
import io.confluent.ksql.function.types.ArrayType;
import io.confluent.ksql.function.types.GenericType;
import io.confluent.ksql.function.types.LambdaType;
import io.confluent.ksql.function.types.MapType;
import io.confluent.ksql.function.types.ParamType;
import io.confluent.ksql.function.types.ParamTypes;
import io.confluent.ksql.function.types.StructType;
import io.confluent.ksql.name.FunctionName;
import io.confluent.ksql.util.KsqlException;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;
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
  public void shouldGetTimestampSchemaForTimestampClass() {
    assertThat(
        UdfUtil.getSchemaFromType(Timestamp.class),
        is(ParamTypes.TIMESTAMP)
    );
  }

  @Test
  public void shouldGetIntervalUnitTypeForTimeUnitClass() {
    assertThat(
        UdfUtil.getSchemaFromType(TimeUnit.class),
        is(ParamTypes.INTERVALUNIT)
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

  @Test(expected = KsqlException.class)
  public void shouldThrowExceptionForObjClass() {
    UdfUtil.getSchemaFromType(Object.class);
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

  @Test
  public void shouldGetFunction() throws NoSuchMethodException {
    final Type type = getClass().getDeclaredMethod("functionType", Function.class)
        .getGenericParameterTypes()[0];
    final ParamType schema = UdfUtil.getSchemaFromType(type);
    assertThat(schema, instanceOf(LambdaType.class));
    assertThat(((LambdaType) schema).inputTypes(), equalTo(ImmutableList.of(ParamTypes.LONG)));
    assertThat(((LambdaType) schema).returnType(), equalTo(ParamTypes.INTEGER));
  }

  @Test
  public void shouldGetPartialGenericFunction() throws NoSuchMethodException {
    // Given:
    final Type genericType = getClass().getMethod("partialGenericFunctionType").getGenericReturnType();

    // When:
    final ParamType returnType = UdfUtil.getSchemaFromType(genericType);

    // Then:
    assertThat(returnType, is(LambdaType.of(ImmutableList.of(ParamTypes.LONG), GenericType.of("U"))));
  }

  @Test
  public void shouldGetGenericFunction() throws NoSuchMethodException {
    // Given:
    final Type genericType = getClass().getMethod("genericFunctionType").getGenericReturnType();

    // When:
    final ParamType returnType = UdfUtil.getSchemaFromType(genericType);

    // Then:
    assertThat(returnType, is(LambdaType.of(ImmutableList.of(GenericType.of("T")), GenericType.of("U"))));
  }

  @Test
  public void shouldGetBiFunction() throws NoSuchMethodException {
    final Type type = getClass().getDeclaredMethod("biFunctionType", BiFunction.class)
        .getGenericParameterTypes()[0];
    final ParamType schema = UdfUtil.getSchemaFromType(type);
    assertThat(schema, instanceOf(LambdaType.class));
    assertThat(((LambdaType) schema).inputTypes(), equalTo(ImmutableList.of(ParamTypes.LONG, ParamTypes.INTEGER)));
    assertThat(((LambdaType) schema).returnType(), equalTo(ParamTypes.BOOLEAN));
  }

  @Test
  public void shouldGetPartialGenericBiFunction() throws NoSuchMethodException {
    // Given:
    final Type genericType = getClass().getMethod("partialGenericBiFunctionType").getGenericReturnType();

    // When:
    final ParamType returnType = UdfUtil.getSchemaFromType(genericType);

    // Then:
    assertThat(returnType, is(LambdaType.of(ImmutableList.of(GenericType.of("T"), ParamTypes.BOOLEAN), GenericType.of("U"))));
  }

  @Test
  public void shouldGetGenericBiFunction() throws NoSuchMethodException {
    // Given:
    final Type genericType = getClass().getMethod("partialGenericBiFunctionType").getGenericReturnType();

    // When:
    final ParamType returnType = UdfUtil.getSchemaFromType(genericType);

    // Then:
    assertThat(returnType, is(LambdaType.of(ImmutableList.of(GenericType.of("T"), ParamTypes.BOOLEAN), GenericType.of("U"))));
  }

  @Test
  public void shouldGetTriFunction() throws NoSuchMethodException {
    final Type type = getClass().getDeclaredMethod("triFunctionType", TriFunction.class)
        .getGenericParameterTypes()[0];
    final ParamType schema = UdfUtil.getSchemaFromType(type);
    assertThat(schema, instanceOf(LambdaType.class));
    assertThat(((LambdaType) schema).inputTypes(), equalTo(ImmutableList.of(ParamTypes.LONG, ParamTypes.INTEGER, ParamTypes.BOOLEAN)));
    assertThat(((LambdaType) schema).returnType(), equalTo(ParamTypes.BOOLEAN));
  }

  @Test
  public void shouldGetPartialGenericTriFunction() throws NoSuchMethodException {
    // Given:
    final Type genericType = getClass().getMethod("partialGenericTriFunctionType").getGenericReturnType();

    // When:
    final ParamType returnType = UdfUtil.getSchemaFromType(genericType);

    // Then:
    assertThat(returnType, is(LambdaType.of(ImmutableList.of(GenericType.of("T"), ParamTypes.BOOLEAN, GenericType.of("U")), ParamTypes.INTEGER)));
  }

  @Test
  public void shouldGetGenericTriFunction() throws NoSuchMethodException {
    // Given:
    final Type genericType = getClass().getMethod("genericTriFunctionType").getGenericReturnType();

    // When:
    final ParamType returnType = UdfUtil.getSchemaFromType(genericType);

    // Then:
    assertThat(returnType, is(LambdaType.of(ImmutableList.of(GenericType.of("T"), GenericType.of("U"), GenericType.of("V")), GenericType.of("W"))));
  }

  @Test
  public void shouldGetObjectSchemaForObjectClassVariadic() {
    assertThat(
            UdfUtil.getVarArgsSchemaFromType(Object.class),
            equalTo(ParamTypes.ANY)
    );
  }

  @Test
  public void shouldGetBooleanSchemaForBooleanClassVariadic() {
    assertThat(
            UdfUtil.getVarArgsSchemaFromType(Boolean.class),
            equalTo(ParamTypes.BOOLEAN)
    );
  }

  @Test
  public void shouldGetBooleanSchemaForBooleanPrimitiveClassVariadic() {
    assertThat(
            UdfUtil.getVarArgsSchemaFromType(boolean.class),
            equalTo(ParamTypes.BOOLEAN)
    );
  }

  @Test
  public void shouldGetIntSchemaForIntegerClassVariadic() {
    assertThat(
            UdfUtil.getVarArgsSchemaFromType(Integer.class),
            equalTo(ParamTypes.INTEGER)
    );
  }

  @Test
  public void shouldGetIntegerSchemaForIntPrimitiveClassVariadic() {
    assertThat(
            UdfUtil.getVarArgsSchemaFromType(int.class),
            equalTo(ParamTypes.INTEGER)
    );
  }

  @Test
  public void shouldGetLongSchemaForLongClassVariadic() {
    assertThat(
            UdfUtil.getVarArgsSchemaFromType(Long.class),
            equalTo(ParamTypes.LONG)
    );
  }

  @Test
  public void shouldGetLongSchemaForLongPrimitiveClassVariadic() {
    assertThat(
            UdfUtil.getVarArgsSchemaFromType(long.class),
            equalTo(ParamTypes.LONG)
    );
  }

  @Test
  public void shouldGetFloatSchemaForDoubleClassVariadic() {
    assertThat(
            UdfUtil.getVarArgsSchemaFromType(Double.class),
            equalTo(ParamTypes.DOUBLE)
    );
  }

  @Test
  public void shouldGetFloatSchemaForDoublePrimitiveClassVariadic() {
    assertThat(
            UdfUtil.getVarArgsSchemaFromType(double.class),
            equalTo(ParamTypes.DOUBLE)
    );
  }

  @Test
  public void shouldGetDecimalSchemaForBigDecimalClassVariadic() {
    assertThat(
            UdfUtil.getVarArgsSchemaFromType(BigDecimal.class),
            is(ParamTypes.DECIMAL)
    );
  }

  @Test
  public void shouldGetTimestampSchemaForTimestampClassVariadic() {
    assertThat(
            UdfUtil.getVarArgsSchemaFromType(Timestamp.class),
            is(ParamTypes.TIMESTAMP)
    );
  }

  @Test
  public void shouldGetIntervalUnitTypeForTimeUnitClassVariadic() {
    assertThat(
            UdfUtil.getVarArgsSchemaFromType(TimeUnit.class),
            is(ParamTypes.INTERVALUNIT)
    );
  }

  @Test
  public void shouldGetMapSchemaFromMapClassVariadic() throws NoSuchMethodException {
    final Type type = getClass().getDeclaredMethod("mapType", Map.class)
            .getGenericParameterTypes()[0];
    final ParamType schema = UdfUtil.getVarArgsSchemaFromType(type);
    assertThat(schema, instanceOf(MapType.class));
    assertThat(((MapType) schema).value(), equalTo(ParamTypes.INTEGER));
  }

  @Test
  public void shouldGetArraySchemaFromListClassVariadic() throws NoSuchMethodException {
    final Type type = getClass().getDeclaredMethod("listType", List.class)
            .getGenericParameterTypes()[0];
    final ParamType schema = UdfUtil.getVarArgsSchemaFromType(type);
    assertThat(schema, instanceOf(ArrayType.class));
    assertThat(((ArrayType) schema).element(), equalTo(ParamTypes.DOUBLE));
  }

  @Test
  public void shouldGetStringSchemaFromStringClassVariadic() {
    assertThat(
            UdfUtil.getVarArgsSchemaFromType(String.class),
            equalTo(ParamTypes.STRING)
    );
  }

  @Test
  public void shouldGetStringSchemaFromStructClassVariadic() {
    assertThat(
            UdfUtil.getVarArgsSchemaFromType(Struct.class),
            equalTo(StructType.ANY_STRUCT)
    );
  }

  @Test(expected = KsqlException.class)
  public void shouldThrowExceptionIfClassDoesntMapToSchemaVariadic() {
    UdfUtil.getVarArgsSchemaFromType(System.class);
  }

  @Test
  public void shouldGetGenericSchemaFromTypeVariadic() throws NoSuchMethodException {
    // Given:
    final Type genericType = getClass().getMethod("genericType").getGenericReturnType();

    // When:
    final ParamType returnType = UdfUtil.getVarArgsSchemaFromType(genericType);

    // Then:
    MatcherAssert.assertThat(returnType, CoreMatchers.is(GenericType.of("T")));
  }

  @Test
  public void shouldGetGenericSchemaFromParameterizedTypeVariadic() throws NoSuchMethodException {
    // Given:
    final Type genericType = getClass().getMethod("genericMapType").getGenericReturnType();

    // When:
    final ParamType returnType = UdfUtil.getVarArgsSchemaFromType(genericType);

    // Then:
    assertThat(returnType, is(MapType.of(GenericType.of("K"), GenericType.of("V"))));
  }

  @Test
  public void shouldGetGenericSchemaFromPartialParameterizedTypeVariadic() throws NoSuchMethodException {
    // Given:
    final Type genericType = getClass().getMethod("partialGenericMapType").getGenericReturnType();

    // When:
    final ParamType returnType = UdfUtil.getVarArgsSchemaFromType(genericType);

    // Then:
    assertThat(returnType, is(MapType.of(ParamTypes.LONG, GenericType.of("V"))));
  }

  @Test
  public void shouldGetFunctionVariadic() throws NoSuchMethodException {
    final Type type = getClass().getDeclaredMethod("functionType", Function.class)
            .getGenericParameterTypes()[0];
    final ParamType schema = UdfUtil.getVarArgsSchemaFromType(type);
    assertThat(schema, instanceOf(LambdaType.class));
    assertThat(((LambdaType) schema).inputTypes(), equalTo(ImmutableList.of(ParamTypes.LONG)));
    assertThat(((LambdaType) schema).returnType(), equalTo(ParamTypes.INTEGER));
  }

  @Test
  public void shouldGetPartialGenericFunctionVariadic() throws NoSuchMethodException {
    // Given:
    final Type genericType = getClass().getMethod("partialGenericFunctionType").getGenericReturnType();

    // When:
    final ParamType returnType = UdfUtil.getVarArgsSchemaFromType(genericType);

    // Then:
    assertThat(returnType, is(LambdaType.of(ImmutableList.of(ParamTypes.LONG), GenericType.of("U"))));
  }

  @Test
  public void shouldGetGenericFunctionVariadic() throws NoSuchMethodException {
    // Given:
    final Type genericType = getClass().getMethod("genericFunctionType").getGenericReturnType();

    // When:
    final ParamType returnType = UdfUtil.getVarArgsSchemaFromType(genericType);

    // Then:
    assertThat(returnType, is(LambdaType.of(ImmutableList.of(GenericType.of("T")), GenericType.of("U"))));
  }

  @Test
  public void shouldGetBiFunctionVariadic() throws NoSuchMethodException {
    final Type type = getClass().getDeclaredMethod("biFunctionType", BiFunction.class)
            .getGenericParameterTypes()[0];
    final ParamType schema = UdfUtil.getVarArgsSchemaFromType(type);
    assertThat(schema, instanceOf(LambdaType.class));
    assertThat(((LambdaType) schema).inputTypes(), equalTo(ImmutableList.of(ParamTypes.LONG, ParamTypes.INTEGER)));
    assertThat(((LambdaType) schema).returnType(), equalTo(ParamTypes.BOOLEAN));
  }

  @Test
  public void shouldGetPartialGenericBiFunctionVariadic() throws NoSuchMethodException {
    // Given:
    final Type genericType = getClass().getMethod("partialGenericBiFunctionType").getGenericReturnType();

    // When:
    final ParamType returnType = UdfUtil.getVarArgsSchemaFromType(genericType);

    // Then:
    assertThat(returnType, is(LambdaType.of(ImmutableList.of(GenericType.of("T"), ParamTypes.BOOLEAN), GenericType.of("U"))));
  }

  @Test
  public void shouldGetGenericBiFunctionVariadic() throws NoSuchMethodException {
    // Given:
    final Type genericType = getClass().getMethod("partialGenericBiFunctionType").getGenericReturnType();

    // When:
    final ParamType returnType = UdfUtil.getVarArgsSchemaFromType(genericType);

    // Then:
    assertThat(returnType, is(LambdaType.of(ImmutableList.of(GenericType.of("T"), ParamTypes.BOOLEAN), GenericType.of("U"))));
  }

  @Test
  public void shouldGetTriFunctionVariadic() throws NoSuchMethodException {
    final Type type = getClass().getDeclaredMethod("triFunctionType", TriFunction.class)
            .getGenericParameterTypes()[0];
    final ParamType schema = UdfUtil.getVarArgsSchemaFromType(type);
    assertThat(schema, instanceOf(LambdaType.class));
    assertThat(((LambdaType) schema).inputTypes(), equalTo(ImmutableList.of(ParamTypes.LONG, ParamTypes.INTEGER, ParamTypes.BOOLEAN)));
    assertThat(((LambdaType) schema).returnType(), equalTo(ParamTypes.BOOLEAN));
  }

  @Test
  public void shouldGetPartialGenericTriFunctionVariadic() throws NoSuchMethodException {
    // Given:
    final Type genericType = getClass().getMethod("partialGenericTriFunctionType").getGenericReturnType();

    // When:
    final ParamType returnType = UdfUtil.getVarArgsSchemaFromType(genericType);

    // Then:
    assertThat(returnType, is(LambdaType.of(ImmutableList.of(GenericType.of("T"), ParamTypes.BOOLEAN, GenericType.of("U")), ParamTypes.INTEGER)));
  }

  @Test
  public void shouldGetGenericTriFunctionVariadic() throws NoSuchMethodException {
    // Given:
    final Type genericType = getClass().getMethod("genericTriFunctionType").getGenericReturnType();

    // When:
    final ParamType returnType = UdfUtil.getVarArgsSchemaFromType(genericType);

    // Then:
    assertThat(returnType, is(LambdaType.of(ImmutableList.of(GenericType.of("T"), GenericType.of("U"), GenericType.of("V")), GenericType.of("W"))));
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

  @SuppressWarnings("unused")
  private void functionType(final Function<Long, Integer> function) {
  }

  @SuppressWarnings({"unused", "WeakerAccess", "MethodMayBeStatic"})
  public <U> Function<Long, U> partialGenericFunctionType() {
    return null;
  }

  @SuppressWarnings({"unused", "WeakerAccess", "MethodMayBeStatic"})
  public <T, U> Function<T, U> genericFunctionType() {
    return null;
  }

  @SuppressWarnings("unused")
  private void biFunctionType(final BiFunction<Long, Integer, Boolean> biFunction) {
  }

  @SuppressWarnings({"unused", "WeakerAccess", "MethodMayBeStatic"})
  public <T, U> BiFunction<T, Boolean, U> partialGenericBiFunctionType() {
    return null;
  }

  @SuppressWarnings({"unused", "WeakerAccess", "MethodMayBeStatic"})
  public <T, U, V> BiFunction<T, U, V> genericBiFunctionType() {
    return null;
  }

  @SuppressWarnings("unused")
  private void triFunctionType(final TriFunction<Long, Integer, Boolean, Boolean> triFunction) {
  }

  @SuppressWarnings({"unused", "WeakerAccess", "MethodMayBeStatic"})
  public <T, U> TriFunction<T, Boolean, U, Integer> partialGenericTriFunctionType() {
    return null;
  }

  @SuppressWarnings({"unused", "WeakerAccess", "MethodMayBeStatic"})
  public <T, U, V, W> TriFunction<T, U, V, W> genericTriFunctionType() {
    return null;
  }
}
