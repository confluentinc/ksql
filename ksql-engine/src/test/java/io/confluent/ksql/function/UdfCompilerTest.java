/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.ksql.function;

import org.junit.Test;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class UdfCompilerTest {

  private final ClassLoader classLoader = UdfCompilerTest.class.getClassLoader();
  private final UdfCompiler udfCompiler = new UdfCompiler();

  @Test
  public void shouldCompileFunctionWithMapArgument() throws NoSuchMethodException {
    final UdfInvoker udf = udfCompiler.compile(getClass().getMethod("udf", Map.class), classLoader);
    assertThat(udf.eval(this, Collections.emptyMap()), equalTo("{}"));
  }

  @Test
  public void shouldCompileFunctionWithListArgument() throws NoSuchMethodException {
    final UdfInvoker udf = udfCompiler.compile(getClass().getMethod("udf", List.class), classLoader);
    assertThat(udf.eval(this, Collections.emptyList()), equalTo("[]"));
  }

  @Test
  public void shouldCompileFunctionWithDoubleArgument() throws NoSuchMethodException {
    final UdfInvoker udf = udfCompiler.compile(getClass().getMethod("udf", Double.class), classLoader);
    assertThat(udf.eval(this, 1), equalTo(1.0));
  }

  @Test
  public void shouldCompileFunctionWithIntegerArgument() throws NoSuchMethodException {
    final UdfInvoker udf = udfCompiler.compile(getClass().getMethod("udf", Integer.class), classLoader);
    assertThat(udf.eval(this, 1), equalTo(1));
  }

  @Test
  public void shouldCompileFunctionWithLongArgument() throws NoSuchMethodException {
    final UdfInvoker udf = udfCompiler.compile(getClass().getMethod("udf", Long.class), classLoader);
    assertThat(udf.eval(this, 1), equalTo(1L));
  }

  @Test
  public void shouldCompileFunctionWithBooleanArgument() throws NoSuchMethodException {
    final UdfInvoker udf = udfCompiler.compile(getClass().getMethod("udf", Boolean.class), classLoader);
    assertThat(udf.eval(this, true), equalTo(true));
  }

  @Test
  public void shouldCompileFunctionWithIntArgument() throws NoSuchMethodException {
    final UdfInvoker udf = udfCompiler.compile(getClass().getMethod("udfPrimitive", int.class), classLoader);
    assertThat(udf.eval(this, 1), equalTo(1));
  }

  @Test
  public void shouldCompileFunctionWithPrimitiveLongArgument() throws NoSuchMethodException {
    final UdfInvoker udf = udfCompiler.compile(getClass().getMethod("udfPrimitive", long.class), classLoader);
    assertThat(udf.eval(this, 1), equalTo(1L));
  }

  @Test
  public void shouldCompileFunctionWithPrimitiveDoubleArgument() throws NoSuchMethodException {
    final UdfInvoker udf = udfCompiler.compile(getClass().getMethod("udfPrimitive", double.class), classLoader);
    assertThat(udf.eval(this, 1), equalTo(1.0));
  }

  @Test
  public void shouldCompileFunctionWithPrimitiveBooleanArgument() throws NoSuchMethodException {
    final UdfInvoker udf = udfCompiler.compile(getClass().getMethod("udfPrimitive", boolean.class), classLoader);
    assertThat(udf.eval(this, true), equalTo(true));
  }

  @Test
  public void shouldCompileFunctionWithStringArgument() throws NoSuchMethodException {
    final UdfInvoker udf = udfCompiler.compile(getClass().getMethod("udf", String.class), classLoader);
    assertThat(udf.eval(this, "foo"), equalTo("foo"));
  }

  @Test
  public void shouldHandleMethodsWithMultipleArguments() throws NoSuchMethodException {
    final UdfInvoker udf = udfCompiler.compile(
        getClass().getMethod("multi", int.class, long.class, double.class),
        classLoader);

    assertThat(udf.eval(this, 1, 2, 3), equalTo(6.0));
  }

  public String udf(final Map<String, Integer> map) {
    return map.toString();
  }

  public String udf(final List<String> list) {
    return list.toString();
  }

  public Double udf(final Double val) {
    return val;
  }

  public Float udf(final Float val) {
    return val;
  }

  public Integer udf(final Integer val) {
    return val;
  }

  public Long udf(final Long val) {
    return val;
  }

  public double udfPrimitive(final double val) {
    return val;
  }

  public float udfPrimitive(final float val) {
    return val;
  }

  public int udfPrimitive(final int val) {
    return val;
  }

  public long udfPrimitive(final long val) {
    return val;
  }

  public boolean udfPrimitive(final boolean val) {
    return val;
  }

  public Boolean udf(final Boolean val) {
    return val;
  }

  public String udf(final String val) {
    return val;
  }

  public double multi(final int i, final long l, final double d) {
    return i * l * d;
  }
}