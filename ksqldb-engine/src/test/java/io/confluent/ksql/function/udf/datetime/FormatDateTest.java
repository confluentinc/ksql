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

public class FormatDateTest {

  private FormatDate udf;

  @Before
  public void setUp() {
    udf = new FormatDate();
  }

  @Test
  public void shouldConvertDateToString() {
    // When:
    final String result = udf.formatDate(Date.valueOf("2014-11-09"), "yyyy-MM-dd");

    // Then:
    assertThat(result, is("2014-11-09"));
  }

  @Test
  public void shouldReturnNullOnNullDate() {
    // When:
    final String result = udf.formatDate(null, "yyyy-MM-dd");

    // Then:
    assertThat(result, is(nullValue()));
  }

  @Test
  public void shouldReturnNullOnNullFormat() {
    // When:
    final String result = udf.formatDate(Date.valueOf("2014-11-09"), null);

    // Then:
    assertThat(result, is(nullValue()));
  }


  @Test
  public void shouldThrowOnUnsupportedFields() {
    // When:
    final Exception e = assertThrows(
        KsqlFunctionException.class,
        () -> udf.formatDate(Date.valueOf("2014-11-09"), "yyyy-MM-dd HH:mm"));

    // Then:
    assertThat(e.getMessage(), is("Failed to format date 2014-11-09 with formatter 'yyyy-MM-dd HH:mm': Unsupported field: HourOfDay"));
  }

  @Test
  public void shouldRoundTripWithStringToDate() {
    final String format = "dd/MM/yyyy'Freya'";
    final ParseDate parseDate = new ParseDate();
    IntStream.range(-10_000, 20_000)
        .parallel()
        .forEach(idx -> {
          final String result = udf.formatDate(new Date(idx * 86400000L), format);
          final Date date = parseDate.parseDate(result, format);
          assertThat(date.getTime(), is(idx * 86400000L));
        });
  }

  @Test
  public void shouldSupportEmbeddedChars() {
    // When:
    final Object result = udf.formatDate(Date.valueOf("2014-11-09"), "yyyy-dd-MM'Fred'");

    // Then:
    assertThat(result, is("2014-09-11Fred"));
  }

  @Test
  public void shouldThrowIfFormatInvalid() {
    // When:
    final Exception e = assertThrows(
        KsqlFunctionException.class,
        () -> udf.formatDate(Date.valueOf("2014-11-09"), "invalid")
    );

    // Then:
    assertThat(e.getMessage(), containsString("Failed to format date 2014-11-09 with formatter 'invalid'"));
  }

  @Test
  public void shouldByThreadSafeAndWorkWithManyDifferentFormatters() {
    IntStream.range(0, 10_000)
        .parallel()
        .forEach(idx -> {
          try {
            final String pattern = "yyyy-MM-dd'X" + idx + "'";
            final String result = udf.formatDate(Date.valueOf("2021-05-18"), pattern);
            assertThat(result, is("2021-05-18X" + idx));
          } catch (final Exception e) {
            fail(e.getMessage());
          }
        });
  }

}
