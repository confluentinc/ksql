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

import static io.confluent.ksql.GenericRow.genericRow;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isEmptyString;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TabularRowTest {

  private boolean shouldWrap;
  private int width;

  @Before
  public void setUp() {
    shouldWrap = false;
    width = 0;
  }

  @Test
  public void shouldFormatHeader() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .keyColumn(ColumnName.of("foo"), SqlTypes.BIGINT)
        .valueColumn(ColumnName.of("bar"), SqlTypes.STRING)
        .build();

    // When:
    final String formatted = TabularRow.createHeader(20, schema.columns(), shouldWrap, width).toString();

    // Then:
    assertThat(formatted, is(""
        + "+--------+--------+\n"
        + "|foo     |bar     |\n"
        + "+--------+--------+"));
  }

  @Test
  public void shouldMultilineFormatHeader() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .keyColumn(ColumnName.of("foo"), SqlTypes.BIGINT)
        .valueColumn(ColumnName.of("bar is a long string"), SqlTypes.STRING)
        .build();

    // When:
    final String formatted = TabularRow.createHeader(20, schema.columns(), shouldWrap, width).toString();

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
    givenWrappingEnabled();

    final GenericRow value = genericRow("foo", "bar");

    // When:
    final String formatted = TabularRow.createRow(20, value, shouldWrap, width).toString();

    // Then:
    assertThat(formatted, is("|foo     |bar     |"));
  }

  @Test
  public void shouldMultilineFormatRow() {
    // Given:
    givenWrappingEnabled();

    final GenericRow value = genericRow("foo", "bar is a long string");

    // When:
    final String formatted = TabularRow.createRow(20, value, shouldWrap, width).toString();

    // Then:
    assertThat(formatted, is(""
        + "|foo     |bar is a|\n"
        + "|        | long st|\n"
        + "|        |ring    |"));
  }

  @Test
  public void shouldClipMultilineFormatRow() {
    // Given:
    givenWrappingDisabled();

    final GenericRow value = genericRow("foo", "bar is a long string");

    // When:
    final String formatted = TabularRow.createRow(20, value, shouldWrap, width).toString();

    // Then:
    assertThat(formatted, is(""
        + "|foo     |bar i...|"));
  }

  @Test
  public void shouldClipMultilineFormatRowWithLotsOfWhitespace() {
    // Given:
    givenWrappingDisabled();

    final GenericRow value = genericRow(
        "foo",
        "bar                                                                               foo"
    );

    // When:
    final String formatted = TabularRow.createRow(20, value, shouldWrap, width).toString();

    // Then:
    assertThat(formatted, is(""
        + "|foo     |bar  ...|"));
  }

  @Test
  public void shouldNotAddEllipsesMultilineFormatRowWithLotsOfWhitespace() {
    // Given:
    givenWrappingDisabled();

    final GenericRow value = genericRow(
        "foo",
        "bar                                                                                  "
    );

    // When:
    final String formatted = TabularRow.createRow(20, value, shouldWrap, width).toString();

    // Then:
    assertThat(formatted, is(""
        + "|foo     |bar     |"));
  }


  @Test
  public void shouldFormatNoColumnsHeader() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .build();

    // When:
    final String formatted = TabularRow.createHeader(20, schema.columns(), shouldWrap, width).toString();

    // Then:
    assertThat(formatted, isEmptyString());
  }

  @Test
  public void shouldFormatMoreColumnsThanWidth() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .keyColumn(ColumnName.of("foo"), SqlTypes.BIGINT)
        .valueColumn(ColumnName.of("bar"), SqlTypes.STRING)
        .valueColumn(ColumnName.of("baz"), SqlTypes.DOUBLE)
        .build();

    // When:
    final String formatted = TabularRow.createHeader(3, schema.columns(), shouldWrap, width).toString();

    // Then:
    assertThat(formatted,
        is(""
            + "+-----+-----+-----+\n"
            + "|foo  |bar  |baz  |\n"
            + "+-----+-----+-----+"));
  }

  @Test
  public void shouldFormatCustomColumnWidth() {
    // Given:
    givenCustomColumnWidth(10);

    final LogicalSchema schema = LogicalSchema.builder()
        .keyColumn(ColumnName.of("foo"), SqlTypes.BIGINT)
        .valueColumn(ColumnName.of("bar"), SqlTypes.STRING)
        .valueColumn(ColumnName.of("baz"), SqlTypes.DOUBLE)
        .build();

    // When:
    final String formatted = TabularRow.createHeader(999, schema.columns(), shouldWrap, width).toString();

    // Then:
    assertThat(formatted,
        is(""
            + "+----------+----------+----------+\n"
            + "|foo       |bar       |baz       |\n"
            + "+----------+----------+----------+"));
  }

  private void givenWrappingEnabled() {
    shouldWrap = true;
  }

  private void givenWrappingDisabled() {
    shouldWrap = false;
  }

  private void givenCustomColumnWidth(int width) {
    this.width = width;
  }
}
