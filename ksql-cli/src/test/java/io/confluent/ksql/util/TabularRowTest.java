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

package io.confluent.ksql.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isEmptyString;

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.junit.Test;

public class TabularRowTest {

  @Test
  public void shouldFormatHeader() {
    // Given:
    final List<String> header = ImmutableList.of("foo", "bar");

    // When:
    final String formatted = new TabularRow(20, header, null).toString();

    // Then:
    assertThat(formatted, is(""
        + "+--------+--------+\n"
        + "|foo     |bar     |\n"
        + "+--------+--------+"));
  }

  @Test
  public void shouldMultilineFormatHeader() {
    // Given:
    final List<String> header = ImmutableList.of("foo", "bar is a long string");

    // When:
    final String formatted = new TabularRow(20, header, null).toString();

    // Then:
    assertThat(formatted, is(""
        + "+--------+--------+\n"
        + "|foo     |bar is a|\n"
        + "|        | long st|\n"
        + "|        |ring    |\n"
        + "+--------+--------+"));
  }

  @Test
  public void shouldFormatRow() {
    // Given:
    final List<String> header = ImmutableList.of("foo", "bar");

    // When:
    final String formatted = new TabularRow(20, header, header).toString();

    // Then:
    assertThat(formatted, is("|foo     |bar     |"));
  }

  @Test
  public void shouldMultilineFormatRow() {
    // Given:
    final List<String> header = ImmutableList.of("foo", "bar is a long string");

    // When:
    final String formatted = new TabularRow(20, header, header).toString();

    // Then:
    assertThat(formatted, is(""
        + "|foo     |bar is a|\n"
        + "|        | long st|\n"
        + "|        |ring    |"));
  }

  @Test
  public void shouldFormatNoColumns() {
    // Given:
    final List<String> header = ImmutableList.of();

    // When:
    final String formatted = new TabularRow(20, header, null).toString();

    // Then:
    assertThat(formatted, isEmptyString());
  }

  @Test
  public void shouldFormatMoreColumnsThanWidth() {
    // Given:
    final List<String> header = ImmutableList.of("foo", "bar", "baz");

    // When:
    final String formatted = new TabularRow(3, header, null).toString();

    // Then:
    assertThat(formatted,
        is(""
            + "+-----+-----+-----+\n"
            + "|foo  |bar  |baz  |\n"
            + "+-----+-----+-----+"));
  }

}