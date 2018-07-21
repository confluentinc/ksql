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
import static org.junit.Assert.assertThat;
import java.util.Arrays;
import java.util.List;

public class ArrayIntersectKudfTest {
  private final ArrayIntersectKudf udf = new ArrayIntersectKudf();

  @SuppressWarnings("rawtypes")
  @Test
  public void shouldIntersectTwoArrays() {
    final List result = udf.intersect(Arrays.asList("foo", " ", "foo", "bar"), Arrays.asList("foo", "baz"));
    assertThat(result, is(Arrays.asList("foo")));
  }

  @SuppressWarnings("rawtypes")
  @Test
  public void shouldIntersectNonStringArrays() {
    final List result = udf.intersect(Arrays.asList(1, 2, 3, 2, 1), Arrays.asList(1, 2, 2));
    assertThat(result, is(Arrays.asList(1, 2)));
  }

  @SuppressWarnings("rawtypes")
  @Test
  public void shouldIntersectMixedContentArrays() {
    final List result = udf.intersect(Arrays.asList("foo", 1, 2, 2.0D, "foo", 2), Arrays.asList(2.0D, "foo"));
    assertThat(result, is(Arrays.asList(2.0, "foo")));
  }

  @SuppressWarnings("rawtypes")
  @Test
  public void shouldReturnNullForNullInput() {
    List result = udf.intersect(null, null);
    assertThat(result, is(nullValue()));
  }

  @SuppressWarnings("rawtypes")
  @Test
  public void shouldIntersectArraysContainingNulls() {
    List result = udf.intersect(Arrays.asList(null, "bar"), Arrays.asList("foo"));
    assertThat(result.isEmpty(), is(true));
  }

  @SuppressWarnings("rawtypes")
  @Test
  public void shouldReturnNullForArraysOfOnlyNulls() {
    List result = udf.intersect(Arrays.asList(null, null), Arrays.asList(null, null));
    assertThat(result.get(0), is(nullValue()));
  }

}
