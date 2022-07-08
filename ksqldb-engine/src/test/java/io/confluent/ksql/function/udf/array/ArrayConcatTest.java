/*
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the
 * License.
 */

package io.confluent.ksql.function.udf.array;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;

import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.junit.Test;

public class ArrayConcatTest {

  private final ArrayConcat udf = new ArrayConcat();

  @Test
  public void shouldConcatArraysOfLikeType() {
    final List<String> input1 = Arrays.asList("foo", " ", "bar");
    final List<String> input2 = Arrays.asList("baz");
    final List<String> result = udf.concat(input1, input2);
    assertThat(result, is(Arrays.asList("foo", " ", "bar", "baz")));
  }

  @Test
  public void shouldReturnDuplicateValues() {
    final List<String> input1 = Arrays.asList("foo", "foo", "bar");
    final List<String> input2 = Arrays.asList("baz", "foo");
    final List<String> result = udf.concat(input1, input2);
    assertThat(result, is(Arrays.asList("foo", "foo", "bar", "baz", "foo")));
  }

  @Test
  public void shouldConcatArraysContainingNulls() {
    final List<String> input1 = Arrays.asList(null, "bar");
    final List<String> input2 = Arrays.asList("foo");
    final List<String> result = udf.concat(input1, input2);
    assertThat(result, is(Arrays.asList(null, "bar", "foo")));
  }

  @Test
  public void shouldConcatArraysBothContainingNulls() {
    final List<String> input1 = Arrays.asList(null, "foo", "bar");
    final List<String> input2 = Arrays.asList("foo", null);
    final List<String> result = udf.concat(input1, input2);
    assertThat(result, is(Arrays.asList(null, "foo", "bar", "foo", null)));
  }

  @Test
  public void shouldConcatArraysOfOnlyNulls() {
    final List<String> input1 = Arrays.asList(null, null);
    final List<String> input2 = Arrays.asList(null, null, null);
    final List<String> result = udf.concat(input1, input2);
    assertThat(result, is(Arrays.asList(null, null, null, null, null)));
  }

  @Test
  public void shouldReturnNonNullForNullRightInput() {
    final List<String> input1 = Arrays.asList("foo");
    final List<String> result = udf.concat(input1, null);
    assertThat(result, is(Arrays.asList("foo")));
  }

  @Test
  public void shouldReturnNullForNullLeftInput() {
    final List<String> input1 = Arrays.asList("foo");
    final List<String> result = udf.concat(null, input1);
    assertThat(result, is(Arrays.asList("foo")));
  }

  @Test
  public void shouldReturnNullForAllNullInputs() {
    final List<Long> result = udf.concat((List<Long>) null, (List<Long>) null);
    assertThat(result, is(nullValue()));
  }
}
