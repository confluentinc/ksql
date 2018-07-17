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
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class MapValuesKudfTest {
  private final MapValuesKudf udf = new MapValuesKudf();

  @SuppressWarnings("rawtypes")
  @Test
  public void happyPath() {
    final Map<String, Object> input = Maps.newHashMap();
    input.put("foo", "bar");
    input.put("baz", "baloney");
    final List result = udf.mapValues(input);
    assertThat(result, is(Arrays.asList("bar", "baloney")));
  }

  @SuppressWarnings("rawtypes")
  @Test
  public void shouldHandleDisparateValueTypes() {
    final Map<String, Object> input = Maps.newHashMap();
    final List<Double> doubleArray =
        Lists.newArrayList(Double.valueOf(12.34), Double.valueOf(56.78));
    input.put("foo", "bar");
    input.put("baz", 42);
    input.put("array", doubleArray);
    final List result = udf.mapValues(input);
    assertThat(result, is(Arrays.asList(doubleArray, "bar", 42)));
  }

  @SuppressWarnings("rawtypes")
  @Test
  public void shouldReturnNullForNullInput() {
    List result = udf.mapValues(null);
    assertThat(result, is(nullValue()));
  }

  @SuppressWarnings("rawtypes")
  @Test
  public void shouldReturnNullsFromMapWithNulls() {
    final Map<String, Object> input = Maps.newHashMap();
    input.put("foo", "bar");
    input.put(null, null);
    input.put("baz", null);
    List result = udf.mapValues(input);
    assertThat(result, is(Arrays.asList(null, "bar", null)));
  }

}
