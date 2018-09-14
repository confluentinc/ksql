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
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;
import java.util.stream.IntStream;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class DateToStringTest {

  private static final long MILLIS_PER_DAY = 24 * 60 * 60 * 1000;
  private static final TimeZone UTC = TimeZone.getTimeZone("UTC");

  private DateToString udf;

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Before
  public void setUp(){
    udf = new DateToString();
  }

  @Test
  public void shouldConvertDateToString() {
    // When:
    final String result = udf.dateToString(16383, "yyyy-MM-dd");

    // Then:
    final String expectedResult = expectedResult(16383, "yyyy-MM-dd");
    assertThat(result, is(expectedResult));
  }

  @Test
  public void shouldRoundTripWithStringToDate() {
    final String format = "dd/MM/yyyy'Freya'";
    final StringToDate stringToDate = new StringToDate();
    IntStream.range(-10_000, 20_000)
        .parallel()
        .forEach(idx -> {
          final String result = udf.dateToString(idx, format);
          final String expectedResult = expectedResult(idx, format);
          assertThat(result, is(expectedResult));

          final int daysSinceEpoch = stringToDate.stringToDate(result, format);
          assertThat(daysSinceEpoch, is(idx));
        });
  }

  @Test
  public void shouldSupportEmbeddedChars() {
    // When:
    final Object result = udf.dateToString(12345, "yyyy-dd-MM'Fred'");

    // Then:
    final String expectedResult = expectedResult(12345, "yyyy-dd-MM'Fred'");
    assertThat(result, is(expectedResult));
  }

  @Test
  public void shouldThrowIfFormatInvalid() {
    expectedException.expect(UncheckedExecutionException.class);
    expectedException.expectMessage("Unknown pattern letter: i");
    udf.dateToString(44444, "invalid");
  }

  @Test
  public void shouldBeThreadSafe() {
    IntStream.range(0, 10_000)
        .parallel()
        .forEach(idx -> {
          shouldConvertDateToString();
          udf.dateToString(55555, "yyyy-MM-dd");
        });
  }

  @Test
  public void shouldWorkWithManyDifferentFormatters() {
    IntStream.range(0, 10_000)
        .parallel()
        .forEach(idx -> {
          try {
            final String pattern = "yyyy-MM-dd'X" + idx + "'";
            final String result = udf.dateToString(idx, pattern);
            final String expectedResult = expectedResult(idx, pattern);
            assertThat(result, is(expectedResult));
          } catch (final Exception e) {
            Assert.fail(e.getMessage());
          }
        });
  }

  private String expectedResult(final int daysSinceEpoch, final String formatPattern) {
    SimpleDateFormat dateFormat = new SimpleDateFormat(formatPattern);
    dateFormat.setCalendar(Calendar.getInstance(UTC));
    return dateFormat.format(new java.util.Date(daysSinceEpoch * MILLIS_PER_DAY));
  }

}
