/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.util.timestamp;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;

import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.KsqlException;
import org.junit.Before;
import org.junit.Test;

public class PartialStringToTimestampParserTest {

  private static final StringToTimestampParser FULL_PARSER =
      new StringToTimestampParser(KsqlConstants.DATE_TIME_PATTERN + "X");

  private PartialStringToTimestampParser parser;

  @Before
  public void init() {
    parser = new PartialStringToTimestampParser();
  }

  @Test
  public void shouldParseYear() {
    // When:
    assertThat(parser.parse("2017"), is(fullParse("2017-01-01T00:00:00.000+0000")));
  }

  @Test
  public void shouldParseYearMonth() {
    // When:
    assertThat(parser.parse("2020-02"), is(fullParse("2020-02-01T00:00:00.000+0000")));
  }

  @Test
  public void shouldParseFullDate() {
    // When:
    assertThat(parser.parse("2020-01-02"), is(fullParse("2020-01-02T00:00:00.000+0000")));
    assertThat(parser.parse("2020-01-02T"), is(fullParse("2020-01-02T00:00:00.000+0000")));
    assertThat(parser.parse("2020-01-02TZ"), is(fullParse("2020-01-02T00:00:00.000+0000")));
  }

  @Test
  public void shouldParseDateWithHour() {
    // When:
    assertThat(parser.parse("2020-12-02T13"), is(fullParse("2020-12-02T13:00:00.000+0000")));
    assertThat(parser.parse("2020-12-02T13Z"), is(fullParse("2020-12-02T13:00:00.000+0000")));
  }

  @Test
  public void shouldParseDateWithHourMinute() {
    // When:
    assertThat(parser.parse("2020-12-02T13:59"), is(fullParse("2020-12-02T13:59:00.000+0000")));
    assertThat(parser.parse("2020-12-02T13:59Z"), is(fullParse("2020-12-02T13:59:00.000+0000")));
  }

  @Test
  public void shouldParseDateWithHourMinuteSecond() {
    // When:
    assertThat(parser.parse("2020-12-02T13:59:58"), is(fullParse("2020-12-02T13:59:58.000+0000")));
    assertThat(parser.parse("2020-12-02T13:59:58Z"), is(fullParse("2020-12-02T13:59:58.000+0000")));
  }

  @Test
  public void shouldParseFullDateTime() {
    // When:
    assertThat(parser.parse("2020-12-02T13:59:58.123"), is(fullParse("2020-12-02T13:59:58.123+0000")));
    assertThat(parser.parse("2020-12-02T13:59:58.123Z"), is(fullParse("2020-12-02T13:59:58.123+0000")));
  }

  @Test
  public void shouldParseDateTimeWithPositiveTimezones() {
    assertThat(parser.parse("2017-11-13T23:59:58.999+0100"), is(1510613998999L));
  }

  @Test
  public void shouldParseDateTimeWithNegativeTimezones() {
    assertThat(parser.parse("2017-11-13T23:59:58.999-0100"), is(1510621198999L));
  }

  @Test
  public void shouldThrowOnIncorrectlyFormattedDateTime() {
    // When:
    final KsqlException e = assertThrows(
        KsqlException.class,
        () -> parser.parse("2017-1-1")
    );

    // Then
    assertThat(e.getMessage(), containsString("Failed to parse timestamp '2017-1-1'"));
  }

  @Test
  public void shouldThrowOnTimezoneParseError() {
    // When:
    final KsqlException e = assertThrows(
        KsqlException.class,
        () -> parser.parse("2017-01-01T00:00:00.000+foo")
    );

    // Then
    assertThat(e.getMessage(), containsString(
        "Failed to parse timestamp '2017-01-01T00:00:00.000+foo'"));
  }

  @Test
  public void shouldIncludeRequiredFormatInErrorMessage() {
    // When:
    final KsqlException e = assertThrows(
        KsqlException.class,
        () -> parser.parse("2017-01-01T00:00:00.000+foo")
    );

    // Then
    assertThat(e.getMessage(), containsString(
        "Required format is: \"yyyy-MM-dd'T'HH:mm:ss.SSS\", "
            + "with an optional numeric 4-digit timezone"));
  }

  @Test
  public void shouldParseToTimestamp() {
    assertThat(parser.parseToTimestamp("2017-11-13T23:59:58").getTime(), is(1510617598000L));
    assertThat(parser.parseToTimestamp("2017-11-13T23:59:58.999-0100").getTime(), is(1510621198999L));
  }

  private static long fullParse(final String text) {
    return FULL_PARSER.parse(text);
  }
}