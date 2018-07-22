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

package io.confluent.ksql.function.udf.array;

import org.junit.Test;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.junit.Assert.assertThat;
import java.util.Arrays;
import java.util.List;

public class ArrayDistinctKudfTest {
  private final ArrayDistinctKudf udf = new ArrayDistinctKudf();

  @SuppressWarnings("unchecked")
  @Test
  public void shouldDistinctArray() {
    final List<Object> result = udf.distinct(Arrays.asList("foo", " ", "foo", "bar"));
    assertThat(result, containsInAnyOrder(" ", "bar", "foo"));
  }

  public void shouldNotChangeDistinctArray() {
    @SuppressWarnings("unchecked")
    final List<Object> result = udf.distinct(Arrays.asList("foo", " ", "bar"));
    assertThat(result, containsInAnyOrder("foo", " ", "bar"));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldDistinctNonStringArray() {
    final List<Object> result = udf.distinct(Arrays.asList(1, 2, 3, 2, 1));
    assertThat(result, containsInAnyOrder(1, 2, 3));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldDistinctMixedContentArray() {
    final List<Object> result = udf.distinct(Arrays.asList("foo", 1, 2, 2.0D, "foo", 2));
    assertThat(result, containsInAnyOrder(2.0, 1, 2, "foo"));
  }


  @SuppressWarnings("rawtypes")
  @Test
  public void shouldReturnNullForNullInput() {
    List result = udf.distinct(null);
    assertThat(result, is(nullValue()));
  }

}
