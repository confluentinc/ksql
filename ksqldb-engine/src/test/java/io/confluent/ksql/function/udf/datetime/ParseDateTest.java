/*
 * Copyright 2021 Confluent Inc.
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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;

import io.confluent.ksql.function.KsqlFunctionException;
import java.sql.Date;
import java.util.stream.IntStream;
import org.junit.Before;
import org.junit.Test;

public class ParseDateTest {

  private ParseDate udf;

  @Before
  public void setUp() {
    udf = new ParseDate();
  }

  @Test
  public void shouldConvertStringToDate() {
    // When:
    final Date result = udf.parseDate("2021-12-01", "yyyy-MM-dd");

    // Then:
    assertThat(result.getTime(), is(1638316800000L));
  }

  @Test
  public void shouldConvertYearMonthToDate() {
    // When:
    final Date result = udf.parseDate("2021-12", "yyyy-MM");

    // Then:
    assertThat(result.getTime(), is(1638316800000L));
  }

  @Test
  public void shouldConvertYearToDate() {
    // When:
    final Date result = udf.parseDate("2022", "yyyy");

    // Then:
    assertThat(result.getTime(), is(1640995200000L));
  }

  @Test
  public void shouldConvertCaseInsensitiveStringToDate() {
    // When:
    final Date result = udf.parseDate("01-dec-2021", "dd-MMM-yyyy");

    // Then:
    assertThat(result.getTime(), is(1638316800000L));
  }

  @Test
  public void shouldThrowOnUnsupportedFields() {
    // When:
    final Exception e = assertThrows(
        KsqlFunctionException.class,
        () -> udf.parseDate("2021-12-01 05:40:34", "yyyy-MM-dd HH:mm:ss"));

    // Then:
    assertThat(e.getMessage(), is("Failed to parse date '2021-12-01 05:40:34' with formatter 'yyyy-MM-dd HH:mm:ss': Date format contains time field."));
  }

  @Test
  public void shouldSupportEmbeddedChars() {
    // When:
    final Date result = udf.parseDate("2021-12-01Fred", "yyyy-MM-dd'Fred'");

    // Then:
    assertThat(result.getTime(), is(1638316800000L));
  }

  @Test
  public void shouldThrowIfFormatInvalid() {
    // When:
    final Exception e = assertThrows(
        KsqlFunctionException.class,
        () -> udf.parseDate("2021-12-01", "invalid")
    );

    // Then:
    assertThat(e.getMessage(), containsString("Failed to parse date '2021-12-01' with formatter 'invalid'"));
  }

  @Test
  public void shouldThrowIfParseFails() {
    // When:
    final Exception e = assertThrows(
        KsqlFunctionException.class,
        () -> udf.parseDate("invalid", "yyyy-MM-dd")
    );

    // Then:
    assertThat(e.getMessage(), containsString("Failed to parse date 'invalid' with formatter 'yyyy-MM-dd'"));
  }

  @Test
  public void shouldThrowOnEmptyString() {
    // When:
    final Exception e = assertThrows(
        KsqlFunctionException.class,
        () -> udf.parseDate("", "yyyy-MM-dd")
    );

    // Then:
    assertThat(e.getMessage(), containsString("Failed to parse date '' with formatter 'yyyy-MM-dd'"));
  }

  @Test
  public void shouldBeThreadSafeAndWorkWithManyDifferentFormatters() {
    IntStream.range(0, 10_000)
        .parallel()
        .forEach(idx -> {
          try {
            final String sourceDate = "2021-12-01X" + idx;
            final String pattern = "yyyy-MM-dd'X" + idx + "'";
            final Date result = udf.parseDate(sourceDate, pattern);
            assertThat(result.getTime(), is(1638316800000L));
          } catch (final Exception e) {
            fail(e.getMessage());
          }
        });
  }

  @Test
  public void shouldHandleNullDate() {
    // When:
    final Date result = udf.parseDate(null, "dd-MMM-yyyy");

    // Then:
    assertThat(result, is(nullValue()));
  }

  @Test
  public void shouldHandleNullDateFormat() {
    // When:
    final Date result = udf.parseDate("2021-12-01", null);

    // Then:
    assertThat(result, is(nullValue()));
  }

}
