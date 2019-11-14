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
import static org.mockito.Mockito.when;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.cli.console.CliConfig;
import io.confluent.ksql.cli.console.CliConfig.OnOff;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TabularRowTest {

  @Mock
  private CliConfig config;

  @Test
  public void shouldFormatHeader() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .noImplicitColumns()
        .keyColumn(ColumnName.of("foo"), SqlTypes.BIGINT)
        .valueColumn(ColumnName.of("bar"), SqlTypes.STRING)
        .build();

    // When:
    final String formatted = TabularRow.createHeader(20, schema).toString();

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
        .noImplicitColumns()
        .keyColumn(ColumnName.of("foo"), SqlTypes.BIGINT)
        .valueColumn(ColumnName.of("bar is a long string"), SqlTypes.STRING)
        .build();

    // When:
    final String formatted = TabularRow.createHeader(20, schema).toString();

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

    final GenericRow value = new GenericRow("foo", "bar");

    // When:
    final String formatted = TabularRow.createRow(20, value, config).toString();

    // Then:
    assertThat(formatted, is("|foo     |bar     |"));
  }

  @Test
  public void shouldMultilineFormatRow() {
    // Given:
    givenWrappingEnabled();

    final GenericRow value = new GenericRow("foo", "bar is a long string");

    // When:
    final String formatted = TabularRow.createRow(20, value, config).toString();

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

    final GenericRow value = new GenericRow("foo", "bar is a long string");

    // When:
    final String formatted = TabularRow.createRow(20, value, config).toString();

    // Then:
    assertThat(formatted, is(""
        + "|foo     |bar i...|"));
  }

  @Test
  public void shouldClipMultilineFormatRowWithLotsOfWhitespace() {
    // Given:
    givenWrappingDisabled();

    final GenericRow value = new GenericRow(
        "foo",
        "bar                                                                               foo"
    );

    // When:
    final String formatted = TabularRow.createRow(20, value, config).toString();

    // Then:
    assertThat(formatted, is(""
        + "|foo     |bar  ...|"));
  }

  @Test
  public void shouldNotAddEllipsesMultilineFormatRowWithLotsOfWhitespace() {
    // Given:
    givenWrappingDisabled();

    final GenericRow value = new GenericRow(
        "foo",
        "bar                                                                                  "
    );

    // When:
    final String formatted = TabularRow.createRow(20, value, config).toString();

    // Then:
    assertThat(formatted, is(""
        + "|foo     |bar     |"));
  }


  @Test
  public void shouldFormatNoColumnsHeader() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .noImplicitColumns()
        .build();

    // When:
    final String formatted = TabularRow.createHeader(20, schema).toString();

    // Then:
    assertThat(formatted, isEmptyString());
  }

  @Test
  public void shouldFormatMoreColumnsThanWidth() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .noImplicitColumns()
        .keyColumn(ColumnName.of("foo"), SqlTypes.BIGINT)
        .valueColumn(ColumnName.of("bar"), SqlTypes.STRING)
        .valueColumn(ColumnName.of("baz"), SqlTypes.DOUBLE)
        .build();

    // When:
    final String formatted = TabularRow.createHeader(3, schema).toString();

    // Then:
    assertThat(formatted,
        is(""
            + "+-----+-----+-----+\n"
            + "|foo  |bar  |baz  |\n"
            + "+-----+-----+-----+"));
  }

  private void givenWrappingEnabled() {
    when(config.getString(CliConfig.WRAP_CONFIG)).thenReturn(OnOff.ON.toString());
  }

  private void givenWrappingDisabled() {
    when(config.getString(CliConfig.WRAP_CONFIG)).thenReturn("Not ON");
  }
}