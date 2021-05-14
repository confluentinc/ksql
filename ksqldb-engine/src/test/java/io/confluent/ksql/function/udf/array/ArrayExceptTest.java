/*
 * Copyright 2021 Confluent Inc.
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

import java.util.Arrays;
import java.util.List;
import org.junit.Test;

public class ArrayExceptTest {
  private final ArrayExcept udf = new ArrayExcept();

  @Test
  public void shouldRemoveExceptions() {
    final List<String> input1 = Arrays.asList("foo", "bar", "baz");
    final List<String> input2 = Arrays.asList("foo", "bar");
    final List<String> result = udf.except(input1, input2);
    assertThat(result, is(Arrays.asList("baz")));
  }

  @Test
  public void shouldRetainOnlyDistinctValues() {
    final List<String> input1 = Arrays.asList("foo", " ", "foo", "bar");
    final List<String> input2 = Arrays.asList("bar");
    final List<String> result = udf.except(input1, input2);
    assertThat(result, contains("foo", " "));
  }

  @Test
  public void shouldReturnEmptyArrayIfAllExcepted() {
    final List<String> input1 = Arrays.asList("foo", " ", "foo", "bar");
    final List<String> input2 = Arrays.asList("bar", " ", "foo", "extra");
    final List<String> result = udf.except(input1, input2);
    assertThat(result.isEmpty(), is(true));
  }

  @Test
  public void shouldExceptEmptyArray() {
    final List<String> input1 = Arrays.asList();
    final List<String> input2 = Arrays.asList("foo");
    final List<String> result = udf.except(input1, input2);
    assertThat(result.isEmpty(), is(true));
  }

  @Test
  public void shouldDistinctValuesForEmptyExceptionArray() {
    final List<String> input1 = Arrays.asList("foo", "foo", "bar", "foo");
    final List<String> input2 = Arrays.asList();
    final List<String> result = udf.except(input1, input2);
    assertThat(result, contains("foo", "bar"));
  }

  @Test
  public void shouldExceptFromArrayContainingNulls() {
    final List<String> input1 = Arrays.asList("foo", null, "foo", "bar");
    final List<String> input2 = Arrays.asList("foo");
    final List<String> result = udf.except(input1, input2);
    assertThat(result, contains(null, "bar"));
  }

  @Test
  public void shouldExceptNulls() {
    final List<String> input1 = Arrays.asList("foo", null, "foo", "bar");
    final List<String> input2 = Arrays.asList(null, "foo");
    final List<String> result = udf.except(input1, input2);
    assertThat(result, contains("bar"));
  }

  @Test
  public void shouldReturnNullForNullLeftInput() {
    final List<String> input1 = Arrays.asList("foo");
    final List<String> result = udf.except(input1, null);
    assertThat(result, is(nullValue()));
  }

  @Test
  public void shouldReturnNullForNullExceptionArray() {
    final List<String> input2 = Arrays.asList("foo");
    final List<String> result = udf.except(null, input2);
    assertThat(result, is(nullValue()));
  }

  @Test
  public void shouldReturnNullForNullInputs() {
    @SuppressWarnings("rawtypes")
    final List result = udf.except(null, null);
    assertThat(result, is(nullValue()));
  }

}