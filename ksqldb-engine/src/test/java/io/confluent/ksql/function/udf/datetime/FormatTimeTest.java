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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;

import io.confluent.ksql.function.KsqlFunctionException;
import java.sql.Time;
import java.util.stream.IntStream;
import org.junit.Before;
import org.junit.Test;

public class FormatTimeTest {

  private FormatTime udf;

  @Before
  public void setUp() {
    udf = new FormatTime();
  }

  @Test
  public void shouldConvertTimeToString() {
    // When:
    final String result = udf.formatTime(new Time(65000), "HHmmss");

    // Then:
    assertThat(result, is("000105"));
  }

  @Test
  public void shouldRejectUnsupportedFields() {
    // When:
    final Exception e = assertThrows(
        KsqlFunctionException.class,
        () -> udf.formatTime(new Time(65000), "yyyy HHmmss"));

    // Then:
    assertThat(e.getMessage(), is("Failed to format time 00:01:05 with formatter 'yyyy HHmmss': Unsupported field: YearOfEra"));
  }

  @Test
  public void shouldReturnNullOnNullTime() {
    // When:
    final String result = udf.formatTime(null, "HHmmss");

    // Then:
    assertThat(result, is(nullValue()));
  }

  @Test
  public void shouldReturnNullOnNullFormat() {
    // When:
    final String result = udf.formatTime(new Time(65000), null);

    // Then:
    assertThat(result, is(nullValue()));
  }

  @Test
  public void shouldSupportEmbeddedChars() {
    // When:
    final Object result = udf.formatTime(new Time(65000), "HH:mm:ss.SSS'Fred'");

    // Then:
    assertThat(result, is("00:01:05.000Fred"));
  }

  @Test
  public void shouldThrowIfFormatInvalid() {
    // When:
    final Exception e = assertThrows(
        KsqlFunctionException.class,
        () -> udf.formatTime(new Time(65000), "invalid")
    );

    // Then:
    assertThat(e.getMessage(), containsString("Failed to format time 00:01:05 with formatter 'invalid'"));
  }

  @Test
  public void shouldByThreadSafeAndWorkWithManyDifferentFormatters() {
    IntStream.range(0, 10_000)
        .parallel()
        .forEach(idx -> {
          try {
            final String pattern = "HH:mm:ss'X" + idx + "'";
            final String result = udf.formatTime(new Time(65000), pattern);
            assertThat(result, is("00:01:05X" + idx));
          } catch (final Exception e) {
            fail(e.getMessage());
          }
        });
  }

  @Test
  public void shoudlReturnNull() {
    // When:
    final Object result = udf.formatTime(null, "HH:mm:ss.SSS");

    // Then:
    assertNull(result);
  }
}
