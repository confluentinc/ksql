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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;

import io.confluent.ksql.function.KsqlFunctionException;
import java.util.stream.IntStream;
import org.junit.Before;
import org.junit.Test;

public class DateToStringTest {

  private DateToString udf;

  @Before
  public void setUp() {
    udf = new DateToString();
  }

  @Test
  public void shouldConvertDateToString() {
    // When:
    final String result = udf.dateToString(16383, "yyyy-MM-dd");

    // Then:
    assertThat(result, is("2014-11-09"));
  }

  @Test
  public void shouldRoundTripWithStringToDate() {
    final String format = "dd/MM/yyyy'Freya'";
    final StringToDate stringToDate = new StringToDate();
    IntStream.range(-10_000, 20_000)
        .parallel()
        .forEach(idx -> {
          final String result = udf.dateToString(idx, format);
          final int daysSinceEpoch = stringToDate.stringToDate(result, format);
          assertThat(daysSinceEpoch, is(idx));
        });
  }

  @Test
  public void shouldSupportEmbeddedChars() {
    // When:
    final Object result = udf.dateToString(12345, "yyyy-dd-MM'Fred'");

    // Then:
    assertThat(result, is("2003-20-10Fred"));
  }

  @Test
  public void shouldReturnNullOnNullFormat() {
    // When:
    final String result = udf.dateToString(16383, null);

    // Then:
    assertThat(result, is(nullValue()));
  }

  @Test
  public void shouldThrowIfFormatInvalid() {
    // When:
    final Exception e = assertThrows(
        KsqlFunctionException.class,
        () -> udf.dateToString(44444, "invalid")
    );

    // Then:
    assertThat(e.getMessage(), containsString("Failed to format date 44444 with formatter 'invalid'"));
  }

  @Test
  public void shouldByThreadSafeAndWorkWithManyDifferentFormatters() {
    IntStream.range(0, 10_000)
        .parallel()
        .forEach(idx -> {
          try {
            final String pattern = "yyyy-MM-dd'X" + idx + "'";
            final String result = udf.dateToString(18765, pattern);
            assertThat(result, is("2021-05-18X" + idx));
          } catch (final Exception e) {
            fail(e.getMessage());
          }
        });
  }

}
