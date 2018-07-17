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

import org.junit.Test;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;
import java.util.List;
import java.util.Map;

public class CardinalityKudfTest {
  private final CardinalityKudf udf = new CardinalityKudf();

  @Test
  public void shouldSizeMap() {
    final Map<String, Object> input = Maps.newHashMap();
    input.put("foo", "bar");
    input.put("baz", "baloney");
    final int result = udf.cardinality(input);
    assertThat(result, is(2));
  }

  @SuppressWarnings("rawtypes")
  @Test
  public void shouldSizeArray() {
    final List input = Lists.newArrayList("foo", "bar");
    final int result = udf.cardinality(input);
    assertThat(result, is(2));
  }

  @Test
  public void shouldHandleDisparateMapValueTypes() {
    final Map<String, Object> input = Maps.newHashMap();
    final List<Double> doubleArray =
        Lists.newArrayList(Double.valueOf(12.34), Double.valueOf(56.78));
    input.put("foo", "bar");
    input.put("42", 1234);
    input.put("array", doubleArray);
    final int result = udf.cardinality(input);
    assertThat(result, is(3));
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  @Test
  public void shouldHandleDisparateArrayValueTypes() {
    final List input = Lists.newArrayList("foo", 1, 2, 2.0D, "foo", 2);
    final List<Double> doubleArray =
        Lists.newArrayList(Double.valueOf(12.34), Double.valueOf(56.78));
    input.add(doubleArray);
    final int result = udf.cardinality(input);
    assertThat(result, is(7));
  }

  @SuppressWarnings("rawtypes")
  @Test
  public void shouldReturnNullForNullInputArray() {
    assertThat(udf.cardinality((List)null), is(nullValue()));
  }

  @Test
  public void shouldReturnNullForNullInputMap() {
    assertThat(udf.cardinality((Map<String, Object>)null), is(nullValue()));
  }

  @Test
  public void shouldCorrectlySizeEmptyMap() {
    final Map<String, Object> input = Maps.newHashMap();
    final int result = udf.cardinality(input);
    assertThat(result, is(0));
  }

  @SuppressWarnings("rawtypes")
  @Test
  public void shouldCorrectlySizeEmptyArray() {
    final List input = Lists.newArrayList();
    final int result = udf.cardinality(input);
    assertThat(result, is(0));
  }

}
