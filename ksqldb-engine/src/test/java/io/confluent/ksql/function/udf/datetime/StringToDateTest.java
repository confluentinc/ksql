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

public class StringToDateTest {

  private StringToDate udf;

  @Before
  public void setUp() {
    udf = new StringToDate();
  }

  @Test
  public void shouldConvertStringToDate() {
    // When:
    final int result = udf.stringToDate("2021-12-01", "yyyy-MM-dd");

    // Then:
    assertThat(result, is(18962));
  }

  @Test
  public void shouldConvertCaseInsensitiveStringToDate() {
    // When:
    final int result = udf.stringToDate("01-dec-2021", "dd-MMM-yyyy");

    // Then:
    assertThat(result, is(18962));
  }

  @Test
  public void shouldSupportEmbeddedChars() {
    // When:
    final Object result = udf.stringToDate("2021-12-01Fred", "yyyy-MM-dd'Fred'");

    // Then:
    assertThat(result, is(18962));
  }

  @Test
  public void shouldThrowIfFormatInvalid() {
    // When:
    final Exception e = assertThrows(
        KsqlFunctionException.class,
        () -> udf.stringToDate("2021-12-01", "invalid")
    );

    // Then:
    assertThat(e.getMessage(), containsString("Failed to parse date '2021-12-01' with formatter 'invalid'"));
  }

  @Test
  public void shouldThrowIfParseFails() {
    // When:
    final Exception e = assertThrows(
        KsqlFunctionException.class,
        () -> udf.stringToDate("invalid", "yyyy-MM-dd")
    );

    // Then:
    assertThat(e.getMessage(), containsString("Failed to parse date 'invalid' with formatter 'yyyy-MM-dd'"));
  }

  @Test
  public void shouldThrowOnEmptyString() {
    // When:
    final Exception e = assertThrows(
        KsqlFunctionException.class,
        () -> udf.stringToDate("", "yyyy-MM-dd")
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
            final int result = udf.stringToDate(sourceDate, pattern);
            assertThat(result, is(18962));
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
        () -> udf.stringToDate(null, "yyyy-MM-dd")
    );

    // Then:
    assertThat(e.getMessage(), containsString("Failed to parse date 'null' with formatter"));
  }

  @Test
  public void shouldThrowOnNullDateFormat() {
    final Exception e = assertThrows(
        KsqlFunctionException.class,
        () -> udf.stringToDate("2021-12-01", null)
    );

    // Then:
    assertThat(e.getMessage(), containsString("Failed to parse date '2021-12-01' with formatter 'null'"));
  }

}
