/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql.function.udtf;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.google.common.collect.Lists;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.Test;

public class CubeTest {

  private final Cube cubeUdtf = new Cube();

  @Test
  public void shouldCubeSingleColumn() {
    // Given:
    final Object[] args = {1};

    // When:
    final List<List<Object>> result = cubeUdtf.cube(Arrays.asList(args));

    // Then:
    assertThat(result.size(), is(2));
    assertThat(result.get(0), is(Collections.singletonList(null)));
    assertThat(result.get(1), is(Lists.newArrayList(1)));
  }

  @Test
  public void shouldCubeSingleNullColumn() {
    // Given:
    final Object[] oneNull = {null};

    // When:
    final List<List<Object>> result = cubeUdtf.cube(Arrays.asList(oneNull));

    // Then:
    assertThat(result.size(), is(1));
    assertThat(result.get(0), is(Arrays.asList(new String[]{null})));
  }

  @Test
  public void shouldCubeColumnsWithDifferentTypes() {
    // Given:
    final Object[] args = {1, "foo"};

    // When:
    final List<List<Object>> result = cubeUdtf.cube(Arrays.asList(args));

    // Then:
    assertThat(result.size(), is(4));
    assertThat(result.get(0), is(Arrays.asList(null, null)));
    assertThat(result.get(1), is(Arrays.asList(null, "foo")));
    assertThat(result.get(2), is(Arrays.asList(1, null)));
    assertThat(result.get(3), is(Arrays.asList(1, "foo")));
  }

  @Test
  public void shouldHandleOneNull() {
    // Given:
    final Object[] oneNull = {1, null};

    // When:
    final List<List<Object>> result = cubeUdtf.cube(Arrays.asList(oneNull));

    // Then:
    assertThat(result.size(), is(2));
    assertThat(result.get(0), is(Arrays.asList(null, null)));
    assertThat(result.get(1), is(Arrays.asList(1, null)));
  }

  @Test
  public void shouldHandleAllNulls() {
    // Given:
    final Object[] allNull = {null, null};

    // When:
    final List<List<Object>> result = cubeUdtf.cube(Arrays.asList(allNull));

    // Then:
    assertThat(result.size(), is(1));
    assertThat(result.get(0), is(Arrays.asList(null, null)));
  }

}
