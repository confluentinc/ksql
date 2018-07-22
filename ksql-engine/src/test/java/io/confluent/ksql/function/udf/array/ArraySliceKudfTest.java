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
import com.google.common.collect.Lists;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.junit.Assert.assertThat;
import java.util.Arrays;
import java.util.List;

public class ArraySliceKudfTest {
  private final ArraySliceKudf udf = new ArraySliceKudf();

  @SuppressWarnings({"unchecked", "rawtypes"})
  @Test
  public void shouldSliceArray() {
    final List input = Lists.newArrayList("foo", " ", "foo", "bar");
    final List<Object> result = udf.slice(input, 1, 2);
    assertThat(result, containsInAnyOrder(" ", "foo"));
    assertThat(result, hasSize(2));
  }

  @SuppressWarnings("unchecked")
  public void shouldReturnWholeArrayForLargeSliceSize() {
    final List<Object> result = udf.slice(Arrays.asList("foo", " ", "bar"), 0, 99);
    assertThat(result, containsInAnyOrder("foo", " ", "bar"));
    assertThat(result, hasSize(3));
  }

  @SuppressWarnings("unchecked")
  public void shouldReturnOriginalInputForNegativeArgs() {
    final List<Object> result = udf.slice(Arrays.asList("foo", " ", "bar"), 2, -1);
    assertThat(result, containsInAnyOrder("foo", " ", "bar"));
    assertThat(result, hasSize(3));
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  @Test
  public void shouldSliceMixedContentArray() {
    final List input = Lists.newArrayList("foo", 1, 2, 2.0D, "foo", 2);
    final List<Object> result = udf.slice(input, 2, 2);
    assertThat(result, containsInAnyOrder(2, 2.0));
    assertThat(result, hasSize(2));
  }


  @SuppressWarnings("rawtypes")
  @Test
  public void shouldReturnNullForNullInput() {
    List result = udf.slice(null, 1, 1);
    assertThat(result, is(nullValue()));
  }

}
