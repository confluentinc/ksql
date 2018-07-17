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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ArrayDistinctKudfTest {
  private final ArrayDistinctKudf udf = new ArrayDistinctKudf();

  @SuppressWarnings("rawtypes")
  @Test
  public void shouldDistinctArray() {
    final List result = udf.distinct(Arrays.asList("foo", " ", "foo", "bar"));
    assertThat(result, is(Arrays.asList(" ", "bar", "foo")));
  }

  @SuppressWarnings("rawtypes")
  public void shouldNotChangeDistinctArray() {
    final List result = udf.distinct(Arrays.asList("foo", " ", "bar"));
    assertThat(result, is(Arrays.asList("foo", " ", "bar")));
  }

  @SuppressWarnings("rawtypes")
  @Test
  public void shouldDistinctNonStringArray() {
    final List result = udf.distinct(Arrays.asList(1, 2, 3, 2, 1));
    assertThat(result, is(Arrays.asList(1, 2, 3)));
  }

  @SuppressWarnings("rawtypes")
  @Test
  public void shouldDistinctMixedContentArray() {
    final List result = udf.distinct(Arrays.asList("foo", 1, 2, 2.0D, "foo", 2));
    assertThat(result, is(Arrays.asList(2.0, 1, 2, "foo")));
  }


  @SuppressWarnings("rawtypes")
  @Test
  public void shouldReturnNullForNullInput() {
    List result = udf.distinct(null);
    assertThat(result, is(nullValue()));
  }

}
