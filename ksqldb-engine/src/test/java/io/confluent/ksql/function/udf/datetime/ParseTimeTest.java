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
import java.sql.Time;
import java.util.stream.IntStream;
import org.junit.Before;
import org.junit.Test;

public class ParseTimeTest {

  private ParseTime udf;

  @Before
  public void setUp() {
    udf = new ParseTime();
  }

  @Test
  public void shouldConvertStringToDate() {
    // When:
    final Time result = udf.parseTime("000105", "HHmmss");

    // Then:
    assertThat(result.getTime(), is(65000L));
  }

  @Test
  public void shouldConvertCaseInsensitiveStringToDate() {
    // When:
    final Time result = udf.parseTime("12:01:05 aM", "hh:mm:ss a");

    // Then:
    assertThat(result.getTime(), is(65000L));
  }

  @Test
  public void shouldThrowOnUnsupportedFields() {
    // When:
    final Exception e = assertThrows(
        KsqlFunctionException.class,
        () -> udf.parseTime("2020 000105", "yyyy HHmmss"));

    // Then:
    assertThat(e.getMessage(), is("Failed to parse time '2020 000105' with formatter 'yyyy HHmmss': Time format contains date field."));
  }

  @Test
  public void shouldSupportEmbeddedChars() {
    // When:
    final Time result = udf.parseTime("000105.000Fred", "HHmmss.SSS'Fred'");

    // Then:
    assertThat(result.getTime(), is(65000L));
  }

  @Test
  public void shouldThrowIfFormatInvalid() {
    // When:
    final Exception e = assertThrows(
        KsqlFunctionException.class,
        () -> udf.parseTime("000105", "invalid")
    );

    // Then:
    assertThat(e.getMessage(), containsString("Failed to parse time '000105' with formatter 'invalid'"));
  }

  @Test
  public void shouldThrowIfParseFails() {
    // When:
    final Exception e = assertThrows(
        KsqlFunctionException.class,
        () -> udf.parseTime("invalid", "HHmmss")
    );

    // Then:
    assertThat(e.getMessage(), containsString("Failed to parse time 'invalid' with formatter 'HHmmss'"));
  }

  @Test
  public void shouldThrowOnEmptyString() {
    // When:
    final Exception e = assertThrows(
        KsqlFunctionException.class,
        () -> udf.parseTime("", "HHmmss")
    );

    // Then:
    assertThat(e.getMessage(), containsString("Failed to parse time '' with formatter 'HHmmss'"));
  }

  @Test
  public void shouldBeThreadSafeAndWorkWithManyDifferentFormatters() {
    IntStream.range(0, 10_000)
        .parallel()
        .forEach(idx -> {
          try {
            final String sourceDate = "000105X" + idx;
            final String pattern = "HHmmss'X" + idx + "'";
            final Time result = udf.parseTime(sourceDate, pattern);
            assertThat(result.getTime(), is(65000L));
          } catch (final Exception e) {
            fail(e.getMessage());
          }
        });
  }

  @Test
  public void shouldHandleNullDate() {
    // When:
    final Time result = udf.parseTime(null, "HHmmss");

    // Then:
    assertThat(result, is(nullValue()));
  }

  @Test
  public void shouldHandleNullFormat() {
    // When:
    final Time result = udf.parseTime("000105", null);

    // Then:
    assertThat(result, is(nullValue()));
  }
}
