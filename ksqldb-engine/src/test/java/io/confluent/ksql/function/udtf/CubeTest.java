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

package io.confluent.ksql.function.udtf;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;

import com.google.common.collect.Lists;
import io.confluent.ksql.function.KsqlFunctionException;
import java.util.ArrayList;
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

  @Test
  public void shouldRejectInputLargerThanMaxColumns() {
    // Given: an input list with one more column than the safe cap. Without the
    // bound, createAllCombinations would compute `1 << columns.size()` which
    // overflows at size >= 31 (Integer.MIN_VALUE -> new ArrayList<>(neg) throws)
    // or returns 1 at size == 32 (a silently wrong one-row cube). Even just
    // below the overflow boundary (e.g. 30) the function would attempt to
    // allocate ~1B ArrayList entries and OOM the server on a malformed query.
    final List<Object> tooMany = new ArrayList<>();
    for (int i = 0; i < Cube.MAX_COLUMNS + 1; i++) {
      tooMany.add(i);
    }

    // When/Then:
    final KsqlFunctionException e = assertThrows(
        KsqlFunctionException.class,
        () -> cubeUdtf.cube(tooMany));
    assertThat(e.getMessage(), containsString("exceeds the maximum of"));
  }

  // Note: the previously-added shouldAcceptInputAtTheMaxColumns test was
  // removed - it allocated 2^MAX_COLUMNS rows of MAX_COLUMNS elements each
  // (~20M boxed Integers) and pushed the engine surefire run past the CI
  // timeout. The cap is already exercised by the reject test above, and
  // happy-path correctness at small column counts is covered by the
  // existing shouldCubeColumnsWithDifferentTypes / shouldHandleOneNull etc.

}
