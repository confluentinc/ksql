/*
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the
 * License.
 */

package io.confluent.ksql.function.udf.string;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

import org.junit.Test;

public class RPadTest {
  private final RPad udf = new RPad();

  @Test
  public void shouldPadInput() {
    final String result = udf.rpad("foo", 7, "Bar");
    assertThat(result, is("fooBarB"));
  }

  @Test
  public void shouldAppendPartialPadding() {
    final String result = udf.rpad("foo", 4, "Bar");
    assertThat(result, is("fooB"));
  }

  @Test
  public void shouldReturnNullForNullInput() {
    final String result = udf.rpad(null, 4, "foo");
    assertThat(result, is(nullValue()));
  }

  @Test
  public void shouldReturnNullForNullPadding() {
    final String result = udf.rpad("foo", 4, null);
    assertThat(result, is(nullValue()));
  }

  @Test
  public void shouldReturnNullForEmptyPadding() {
    final String result = udf.rpad("foo", 4, "");
    assertThat(result, is(nullValue()));
  }

  @Test
  public void shouldPadEmptyInput() {
    final String result = udf.rpad("", 4, "foo");
    assertThat(result, is("foof"));
  }

  @Test
  public void shouldTruncateInputIfTargetLengthTooSmall() {
    final String result = udf.rpad("foo", 2, "bar");
    assertThat(result, is("fo"));
  }

  @Test
  public void shouldReturnNullForNegativeLength() {
    final String result = udf.rpad("foo", -1, "bar");
    assertThat(result, is(nullValue()));
  }

  @Test
  public void shouldReturnEmptyStringForZeroLength() {
    final String result = udf.rpad("foo", 0, "bar");
    assertThat(result, is(""));
  }

  @Test
  public void shouldReturnNullForNullLength() {
    final String result = udf.rpad("foo", null, "bar");
    assertThat(result, is(nullValue()));
  }

}
