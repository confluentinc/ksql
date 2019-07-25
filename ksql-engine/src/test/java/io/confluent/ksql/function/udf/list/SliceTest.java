/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.function.udf.list;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import com.google.common.collect.Lists;
import java.util.List;
import org.junit.Test;

public class SliceTest {

  @Test
  public void shouldOneIndexSlice() {
    // Given:
    final List<String> list = Lists.newArrayList("a", "b", "c");

    // When:
    final List<String> slice = new Slice().slice(list, 1, 2);

    // Then:
    assertThat(slice, is(Lists.newArrayList("a", "b")));
  }

  @Test
  public void shouldFullListOnNullEndpoints() {
    // Given:
    final List<String> list = Lists.newArrayList("a", "b", "c");

    // When:
    final List<String> slice = new Slice().slice(list, null, null);

    // Then:
    assertThat(slice, is(Lists.newArrayList("a", "b", "c")));
  }

  @Test
  public void shouldOneElementSlice() {
    // Given:
    final List<String> list = Lists.newArrayList("a", "b", "c");

    // When:
    final List<String> slice = new Slice().slice(list, 2, 2);

    // Then:
    assertThat(slice, is(Lists.newArrayList("b")));
  }

  @Test
  public void shouldHandleIntegers() {
    // Given:
    final List<Integer> list = Lists.newArrayList(1, 2, 3);

    // When:
    final List<Integer> slice = new Slice().slice(list, 2, 2);

    // Then:
    assertThat(slice, is(Lists.newArrayList(2)));
  }

  @Test
  public void shouldReturnNullOnIndexError() {
    // Given:
    final List<String> list = Lists.newArrayList("a", "b", "c");

    // When:
    final List<String> slice = new Slice().slice(list, 2, 5);

    // Then:
    assertThat(slice, nullValue());
  }

  @Test
  public void shouldReturnNullOnNullInput() {
    // Given:
    final List<String> list = null;

    // When:
    final List<String> slice = new Slice().slice(list, 1, 2);

    // Then:
    assertThat(slice, nullValue());
  }

}