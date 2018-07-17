/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package io.confluent.ksql.function.udf.map;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import com.google.common.collect.Lists;
import io.confluent.ksql.function.KsqlFunctionException;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;
import java.util.List;
import java.util.Map;

public class ArraysToMapKudfTest {
  private final ArraysToMapKudf udf = new ArraysToMapKudf();

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Test
  public void happyPath() {
    final List<String> keys = Lists.newArrayList("foo", "bar");
    final List<Object> values = Lists.newArrayList("foo1", "bar1");
    final Map<String, Object> result = udf.map(keys, values);
    assertThat(result.size(), is(2));
    assertThat(result.get("foo"), is("foo1"));
    assertThat(result.get("bar"), is("bar1"));
  }

  @Test
  public void shouldHandleDisparateValueTypes() {
    final List<String> keys = Lists.newArrayList("string", "array", "float", "int");
    final List<Double> doubleArray =
        Lists.newArrayList(Double.valueOf(12.34), Double.valueOf(56.78));
    final List<Object> values = Lists.newArrayList("foo1", doubleArray, 0.5f, 42);
    final Map<String, Object> result = udf.map(keys, values);
    assertThat(result.size(), is(4));
    assertThat(result.get("string"), is("foo1"));
    assertThat(result.get("array"), is(doubleArray));
    assertThat(result.get("float"), is(0.5f));
    assertThat(result.get("int"), is(42));
  }

  @Test
  public void shouldFailUnequalLengthInputs() {
    final List<String> keys = Lists.newArrayList("foo", "bar");
    final List<Object> values = Lists.newArrayList("foo1");
    expectedException.expect(KsqlFunctionException.class);
    expectedException.expectMessage("must be of equal length");
    udf.map(keys, values);
  }

  @Test
  public void shouldReturnMapForArraysOfNull() {
    final List<String> keys = Lists.newArrayList((String)null);
    final List<Object> values = Lists.newArrayList((Object)null);
    final Map<String, Object> result = udf.map(keys, values);
    assertThat(result.size(), is(1));
    assertThat(result.containsKey(null), is(true));
    assertThat(result.get(null), is(nullValue()));
  }

  @Test
  public void shouldReturnNullForNullInputs() {
    final Map<String, Object> result = udf.map(null, null);
    assertThat(result, is(nullValue()));
  }

  @Test
  public void shouldAllowNullValueForNonNullKey() {
    final List<String> keys = Lists.newArrayList("foo", "bar");
    final List<Object> values = Lists.newArrayList("foo1", null);
    final Map<String, Object> result = udf.map(keys, values);
    assertThat(result.get("bar"), is(nullValue()));
    assertThat(result.size(), is(2));
  }

}
