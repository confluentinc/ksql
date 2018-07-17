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
import com.google.common.collect.Maps;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;
import java.util.Map;

public class MapUnionKudfTest {
  private final MapUnionKudf udf = new MapUnionKudf();

  @SuppressWarnings("rawtypes")
  @Test
  public void shouldUnionNonEmptyMaps() {
    final Map<String, Object> input1 = Maps.newHashMap();
    input1.put("foo", "bar");
    input1.put("baz", "baloney");
    final Map<String, Object> input2 = Maps.newHashMap();
    input2.put("one", 1);
    input2.put("two", 2);
    input2.put("three", 3);
    final Map result = udf.union(input1, input2);
    assertThat(result.size(), is(5));
    assertThat(result.get("foo"), is("bar"));
    assertThat(result.get("two"), is(2));
  }

  @SuppressWarnings("rawtypes")
  @Test
  public void shouldReturnNullForNullInput() {
    final Map result = udf.union(null, null);
    assertThat(result, is(nullValue()));
  }

}
