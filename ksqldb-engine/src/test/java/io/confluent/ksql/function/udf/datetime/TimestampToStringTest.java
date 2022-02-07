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
import static org.hamcrest.Matchers.either;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;

import io.confluent.ksql.function.KsqlFunctionException;
import io.confluent.ksql.util.KsqlException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.stream.IntStream;
import org.junit.Before;
import org.junit.Test;

public class TimestampToStringTest {

  private TimestampToString udf;

  @Before
  public void setUp() {
    udf = new TimestampToString();
  }

  @Test
  public void shouldConvertTimestampToString() {
    // When:
    final String result = udf.timestampToString(1638360611123L,
        "yyyy-MM-dd HH:mm:ss.SSS");

    // Then:
    final String expectedResult = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
        .format(new Date(1638360611123L));
    assertThat(result, is(expectedResult));
  }

  @Test
  public void testUTCTimeZone() {
    // When:
    final String result = udf.timestampToString(1534353043000L,
        "yyyy-MM-dd HH:mm:ss", "UTC");

    // Then:
    assertThat(result, is("2018-08-15 17:10:43"));
  }

  @Test
  public void testPSTTimeZone() {
    // When:
    final String result = udf.timestampToString(1534353043000L,
        "yyyy-MM-dd HH:mm:ss", "America/Los_Angeles");

    // Then:
    assertThat(result, is("2018-08-15 10:10:43"));
  }

  @Test
  public void testTimeZoneInLocalTime() {
    // Given:
    final long timestamp = 1534353043000L;

    // When:
    final String localTime = udf.timestampToString(timestamp, "yyyy-MM-dd HH:mm:ss zz");

    // Then:
    final String expected = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss zz")
        .format(new Date(timestamp));

    assertThat(localTime, is(expected));
  }

  @Test
  public void testTimeZoneInPacificTime() {
    // Given:
    final long timestamp = 1534353043000L;

    // When:
    final String pacificTime = udf.timestampToString(timestamp,
        "yyyy-MM-dd HH:mm:ss zz", "America/Los_Angeles");

    // Then:
    assertThat(pacificTime,
        either(is("2018-08-15 10:10:43 PDT"))           // Java 8 and below.
            .or(is("2018-08-15 10:10:43 GMT-07:00")));  // Java 9 and above.
  }

  @Test
  public void testTimeZoneInUniversalTime() {
    // Given:
    final long timestamp = 1534353043000L;

    // When:
    final String universalTime = udf.timestampToString(timestamp,
        "yyyy-MM-dd HH:mm:ss zz", "UTC");

    // Then:
    assertThat(universalTime, is("2018-08-15 17:10:43 UTC"));
  }

  @Test
  public void testReturnNullForNullFormat() {
    // When:
    final String result = udf.timestampToString(1534353043000L,
        null);

    // Then:
    assertThat(result, is(nullValue()));
  }

  @Test
  public void testReturnNullForNullTimeZone() {
    // When:
    final String result = udf.timestampToString(1534353043000L,
        "yyyy-MM-dd HH:mm:ss zz", null);

    // Then:
    assertThat(result, is(nullValue()));
  }

  @Test
  public void shouldThrowIfInvalidTimeZone() {
    // When:
    final KsqlException e = assertThrows(
        KsqlFunctionException.class,
        () -> udf.timestampToString(1638360611123L,
        "yyyy-MM-dd HH:mm:ss.SSS", "PST")
    );

    // Then:
    assertThat(e.getMessage(), containsString("Unknown time-zone ID: PST"));
  }

  @Test
  public void shouldSupportEmbeddedChars() {
    // When:
    final Object result = udf.timestampToString(1638360611123L,
        "yyyy-MM-dd'T'HH:mm:ss.SSS'Fred'");

    // Then:
    final String expectedResult = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Fred'")
        .format(new Date(1638360611123L));
    assertThat(result, is(expectedResult));
  }

  @Test
  public void shouldThrowIfFormatInvalid() {
    // When:
    final KsqlException e = assertThrows(
        KsqlFunctionException.class,
        () -> udf.timestampToString(1638360611123L, "invalid")
    );

    // Then:
    assertThat(e.getMessage(), containsString("Unknown pattern letter: i"));
  }

  @Test
  public void shouldBeThreadSafe() {
    IntStream.range(0, 10_000)
        .parallel()
        .forEach(idx -> {
          shouldConvertTimestampToString();
          udf.timestampToString(1538361611123L, "yyyy-MM-dd HH:mm:ss.SSS");
        });
  }

  @Test
  public void shouldWorkWithManyDifferentFormatters() {
    IntStream.range(0, 10_000)
        .parallel()
        .forEach(idx -> {
          try {
            final String pattern = "yyyy-MM-dd HH:mm:ss.SSS'X" + idx + "'";
            final long millis = 1538361611123L + idx;
            final String result = udf.timestampToString(millis, pattern);
            final String expectedResult = new SimpleDateFormat(pattern).format(new Date(millis));
            assertThat(result, is(expectedResult));
          } catch (final Exception e) {
            fail(e.getMessage());
          }
        });
  }

  @Test
  public void shouldRoundTripWithStringToTimestamp() {
    final String pattern = "yyyy-MM-dd HH:mm:ss.SSS'Freya'";
    final StringToTimestamp stringToTimestamp = new StringToTimestamp();
    IntStream.range(-10_000, 20_000)
        .parallel()
        .forEach(idx -> {
          final long millis = 1538361611123L + idx;
          final String result = udf.timestampToString(millis, pattern);
          final String expectedResult = new SimpleDateFormat(pattern).format(new Date(millis));
          assertThat(result, is(expectedResult));

          final long roundtripMillis = stringToTimestamp.stringToTimestamp(result, pattern);
          assertThat(roundtripMillis, is(millis));
        });
  }

  @Test
  public void shouldBehaveLikeSimpleDateFormat() {
    assertLikeSimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    assertLikeSimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSX");
    assertLikeSimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS X");
    assertLikeSimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS z");
    assertLikeSimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS Z");
    assertLikeSimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS XX");
    assertLikeSimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS zz");
    assertLikeSimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS ZZ");
    assertLikeSimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS XXX");
    assertLikeSimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS zzz");
    assertLikeSimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS ZZZ");
    assertLikeSimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    assertLikeSimpleDateFormat("yyyy-MM-dd HH:mm:ss X");
    assertLikeSimpleDateFormat("yyyy-MM-dd HH:mm");
    assertLikeSimpleDateFormat("yyyy-MM-dd HH:mm X");
    assertLikeSimpleDateFormat("yyyy-MM-dd HH");
    assertLikeSimpleDateFormat("yyyy-MM-dd HH X");
    assertLikeSimpleDateFormat("yyyy-MM-dd");
    assertLikeSimpleDateFormat("yyyy-MM-dd z");
    assertLikeSimpleDateFormat("yyyy-MM");
    assertLikeSimpleDateFormat("yyyy");
    assertLikeSimpleDateFormat("MM");
    assertLikeSimpleDateFormat("dd");
    assertLikeSimpleDateFormat("HH");
    assertLikeSimpleDateFormat("mm");
  }

  private void assertLikeSimpleDateFormat(final String format) {
    final String expected = new SimpleDateFormat(format).format(1538361611123L);
    final Object result = new TimestampToString()
        .timestampToString(1538361611123L, format);
    assertThat(result, is(expected));
  }
}
