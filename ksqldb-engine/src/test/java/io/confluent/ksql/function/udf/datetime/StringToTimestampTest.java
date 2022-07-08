/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.function.udf.datetime;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;

import io.confluent.ksql.function.KsqlFunctionException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.stream.IntStream;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

public class StringToTimestampTest {

  private StringToTimestamp udf;

  @Before
  public void setUp() {
    udf = new StringToTimestamp();
  }

  @Test
  public void shouldConvertStringToTimestamp() throws ParseException {
    // When:
    final Object result = udf.stringToTimestamp("2021-12-01 12:10:11.123",
        "yyyy-MM-dd HH:mm:ss.SSS");

    // Then:
    final long expectedResult = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
        .parse("2021-12-01 12:10:11.123").getTime();
    assertThat(result, is(expectedResult));
  }

  @Test
  public void shouldSupportEmbeddedChars() throws ParseException {
    // When:
    final Object result = udf.stringToTimestamp("2021-12-01T12:10:11.123Fred",
        "yyyy-MM-dd'T'HH:mm:ss.SSS'Fred'");

    // Then:
    final long expectedResult = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Fred'")
        .parse("2021-12-01T12:10:11.123Fred").getTime();
    assertThat(result, is(expectedResult));
  }

  @Test
  public void shouldSupportUTCTimeZone() {
    // When:
    final Object result = udf.stringToTimestamp("2018-08-15 17:10:43",
        "yyyy-MM-dd HH:mm:ss", "UTC");

    // Then:
    assertThat(result, is(1534353043000L));
  }

  @Test
  public void shouldSupportPSTTimeZone() {
    // When:
    final Object result = udf.stringToTimestamp("2018-08-15 10:10:43",
        "yyyy-MM-dd HH:mm:ss", "America/Los_Angeles");

    // Then:
    assertThat(result, is(1534353043000L));
  }

  @Test
  public void shouldThrowIfFormatInvalid() {
    // When:
    final KsqlFunctionException e = assertThrows(
        KsqlFunctionException.class,
        () -> udf.stringToTimestamp("2021-12-01 12:10:11.123", "invalid")
    );

    // Then:
    assertThat(e.getMessage(), containsString("Unknown pattern letter: i"));
  }

  @Test
  public void shouldThrowIfParseFails() {
    // When:
    final KsqlFunctionException e = assertThrows(
        KsqlFunctionException.class,
        () -> udf.stringToTimestamp("invalid", "yyyy-MM-dd'T'HH:mm:ss.SSS")
    );

    // Then:
    assertThat(e.getMessage(), containsString("Text 'invalid' could not be parsed at index 0"));
  }

  @Test
  public void shouldThrowOnEmptyString() {
    // When:
    final KsqlFunctionException e = assertThrows(
        KsqlFunctionException.class,
        () -> udf.stringToTimestamp("", "yyyy-MM-dd'T'HH:mm:ss.SSS")
    );

    // Then:
    assertThat(e.getMessage(), containsString("Text '' could not be parsed at index 0"));
  }

  @Test
  public void shouldBeThreadSafe() {
    IntStream.range(0, 10_000)
        .parallel()
        .forEach(idx -> {
          try {
            shouldConvertStringToTimestamp();
          } catch (final ParseException e) {
            fail(e.getMessage());
          }
          udf.stringToTimestamp("1988-01-12 10:12:13.456",
              "yyyy-MM-dd HH:mm:ss.SSS");
        });
  }

  @Test
  public void shouldWorkWithManyDifferentFormatters() {
    IntStream.range(0, 10_000)
        .parallel()
        .forEach(idx -> {
          try {
            final String sourceDate = "2018-12-01 10:12:13.456X" + idx;
            final String pattern = "yyyy-MM-dd HH:mm:ss.SSS'X" + idx + "'";
            final long result = udf.stringToTimestamp(sourceDate, pattern);
            final long expectedResult = new SimpleDateFormat(pattern).parse(sourceDate).getTime();
            assertThat(result, is(expectedResult));
          } catch (final Exception e) {
            fail(e.getMessage());
          }
        });
  }

  @Test
  public void shouldThrowOnNullDate() {
    // When:
    final Exception e = assertThrows(
        KsqlFunctionException.class,
        () -> udf.stringToTimestamp(null, "yyyy-MM-dd")
    );

    // Then:
    assertThat(e.getMessage(), Matchers.containsString("Failed to parse timestamp 'null' with formatter"));
  }

  @Test
  public void shouldThrowOnNullDateFormat() {
    final Exception e = assertThrows(
        KsqlFunctionException.class,
        () -> udf.stringToTimestamp("2021-12-01", null)
    );

    // Then:
    assertThat(e.getMessage(), Matchers.containsString("Failed to parse timestamp '2021-12-01' with formatter 'null'"));
  }

  @Test
  public void shouldBehaveLikeSimpleDateFormat() throws Exception {
    assertLikeSimpleDateFormat("2021-12-01 12:10:11.123", "yyyy-MM-dd HH:mm:ss.SSS");
    assertLikeSimpleDateFormat("2021-12-01 12:10:11.123-0800", "yyyy-MM-dd HH:mm:ss.SSSX");
    assertLikeSimpleDateFormat("2021-12-01 12:10:11.123 +00", "yyyy-MM-dd HH:mm:ss.SSS X");
    assertLikeSimpleDateFormat("2021-12-01 12:10:11.123 PST", "yyyy-MM-dd HH:mm:ss.SSS z");
    assertLikeSimpleDateFormat("2021-12-01 12:10:11.123 -0800", "yyyy-MM-dd HH:mm:ss.SSS Z");
    assertLikeSimpleDateFormat("2021-12-01 12:10:11", "yyyy-MM-dd HH:mm:ss");
    assertLikeSimpleDateFormat("2021-12-01 12:10:11 -0700", "yyyy-MM-dd HH:mm:ss X");
    assertLikeSimpleDateFormat("2021-12-01 12:10", "yyyy-MM-dd HH:mm");
    assertLikeSimpleDateFormat("2021-12-01 12:10 -0700", "yyyy-MM-dd HH:mm X");
    assertLikeSimpleDateFormat("2021-12-01 12", "yyyy-MM-dd HH");
    assertLikeSimpleDateFormat("2021-12-01 12 -0700", "yyyy-MM-dd HH X");
    assertLikeSimpleDateFormat("2021-12-01", "yyyy-MM-dd");
    assertLikeSimpleDateFormat("2021-12-01 PST", "yyyy-MM-dd z");
    assertLikeSimpleDateFormat("2021-12-01 UTC", "yyyy-MM-dd z");
    assertLikeSimpleDateFormat("2021-12", "yyyy-MM");
    assertLikeSimpleDateFormat("2021", "yyyy");
    assertLikeSimpleDateFormat("12", "MM");
    assertLikeSimpleDateFormat("01", "dd");
    assertLikeSimpleDateFormat("01", "HH");
    assertLikeSimpleDateFormat("01", "mm");
  }

  private void assertLikeSimpleDateFormat(final String value, final String format) throws Exception {
    final long expected = new SimpleDateFormat(format).parse(value).getTime();
    final Object result = new StringToTimestamp().stringToTimestamp(value, format);
    assertThat(result, is(expected));
  }

}
