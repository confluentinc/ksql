/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.ksql.function.udf.datetime;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import com.google.common.util.concurrent.UncheckedExecutionException;
import io.confluent.ksql.function.KsqlFunctionException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.format.DateTimeParseException;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;
import java.util.stream.IntStream;
import org.apache.kafka.connect.errors.DataException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class StringToDateTest {

  private static final long MILLIS_PER_DAY = 24 * 60 * 60 * 1000;
  private static final TimeZone UTC = TimeZone.getTimeZone("UTC");

  private StringToDate udf;

  @Before
  public void setUp(){
    udf = new StringToDate();
  }

  @Test
  public void shouldConvertStringToDate() throws ParseException {
    // When:
    final int result = udf.stringToDate("2021-12-01", "yyyy-MM-dd");

    // Then:
    final int expectedResult = expectedResult("2021-12-01", "yyyy-MM-dd");
    assertThat(result, is(expectedResult));
  }

  @Test
  public void shouldSupportEmbeddedChars() throws ParseException {
    // When:
    final Object result = udf.stringToDate("2021-12-01Fred", "yyyy-MM-dd'Fred'");

    // Then:
    final int expectedResult = expectedResult("2021-12-01Fred", "yyyy-MM-dd'Fred'");
    assertThat(result, is(expectedResult));
  }

  @Test(expected = UncheckedExecutionException.class)
  public void shouldThrowIfFormatInvalid() {
    udf.stringToDate("2021-12-01", "invalid");
  }

  @Test(expected = DateTimeParseException.class)
  public void shouldThrowIfParseFails() {
    udf.stringToDate("invalid", "yyyy-MM-dd");
  }

  @Test(expected = DateTimeParseException.class)
  public void shouldThrowOnEmptyString() {
    udf.stringToDate("", "yyyy-MM-dd");
  }

  @Test
  public void shouldBeThreadSafe() {
    IntStream.range(0, 10_000)
        .parallel()
        .forEach(idx -> {
          try {
            shouldConvertStringToDate();
          } catch (final Exception e) {
            Assert.fail(e.getMessage());
          }
          udf.stringToDate("1988-01-12", "yyyy-MM-dd");
        });
  }

  @Test
  public void shouldWorkWithManyDifferentFormatters() {
    IntStream.range(0, 10_000)
        .parallel()
        .forEach(idx -> {
          try {
            final String sourceDate = "2021-12-01X" + idx;
            final String pattern = "yyyy-MM-dd'X" + idx + "'";
            final int result = udf.stringToDate(sourceDate, pattern);
            final int expectedResult = expectedResult(sourceDate, pattern);
            assertThat(result, is(expectedResult));
          } catch (final Exception e) {
            Assert.fail(e.getMessage());
          }
        });
  }


  private int expectedResult(final String formattedDate, final String formatPattern) throws ParseException {
    SimpleDateFormat dateFormat = new SimpleDateFormat(formatPattern);
    dateFormat.setCalendar(Calendar.getInstance(UTC));
    Date parsedDate = dateFormat.parse(formattedDate);
    Calendar calendar = Calendar.getInstance(UTC);
    calendar.setTime(parsedDate);
    if (calendar.get(Calendar.HOUR_OF_DAY) != 0 || calendar.get(Calendar.MINUTE) != 0 ||
        calendar.get(Calendar.SECOND) != 0 || calendar.get(Calendar.MILLISECOND) != 0) {
      fail("Date should not have any time fields set to non-zero values.");
    }
    return (int)(calendar.getTimeInMillis() / MILLIS_PER_DAY);
  }

}