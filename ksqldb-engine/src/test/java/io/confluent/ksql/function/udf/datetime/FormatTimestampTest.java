/*
 * Copyright 2020 Confluent Inc.
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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;

import io.confluent.ksql.function.KsqlFunctionException;
import io.confluent.ksql.util.KsqlException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import java.util.stream.IntStream;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

public class FormatTimestampTest {
  private FormatTimestamp udf;

  @Before
  public void setUp() {
    udf = new FormatTimestamp();
  }

  @Test
  public void shouldConvertTimestampToString() {
    // When:
    final String result = udf.formatTimestamp(new Timestamp(1638360611123L),
        "yyyy-MM-dd HH:mm:ss.SSS");
    final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    sdf.setTimeZone(TimeZone.getTimeZone("GMT"));

    // Then:
    final String expectedResult = sdf.format(new Date(1638360611123L));
    assertThat(result, is(expectedResult));
  }

  @Test
  public void testUTCTimeZone() {
    // When:
    final String result = udf.formatTimestamp(new Timestamp(1534353043000L),
        "yyyy-MM-dd HH:mm:ss", "UTC");

    // Then:
    assertThat(result, is("2018-08-15 17:10:43"));
  }

  @Test
  public void testPSTTimeZone() {
    // When:
    final String result = udf.formatTimestamp(new Timestamp(1534353043000L),
        "yyyy-MM-dd HH:mm:ss", "America/Los_Angeles");

    // Then:
    assertThat(result, is("2018-08-15 10:10:43"));
  }

  @Test
  public void testTimeZoneInPacificTime() {
    // Given:
    final Timestamp timestamp = new Timestamp(1534353043000L);

    // When:
    final String pacificTime = udf.formatTimestamp(timestamp,
        "yyyy-MM-dd HH:mm:ss zz", "America/Los_Angeles");

    // Then:
    assertThat(pacificTime,
        either(is("2018-08-15 10:10:43 PDT"))           // Java 8 and below.
            .or(is("2018-08-15 10:10:43 GMT-07:00")));  // Java 9 and above.
  }

  @Test
  public void testTimeZoneInUniversalTime() {
    // Given:
    final Timestamp timestamp = new Timestamp(1534353043000L);

    // When:
    final String universalTime = udf.formatTimestamp(timestamp,
        "yyyy-MM-dd HH:mm:ss zz", "UTC");

    // Then:
    assertThat(universalTime, is("2018-08-15 17:10:43 UTC"));
  }

  @Test
  public void shouldReturnNullOnNullDate() {
    // When:
    final String returnValue = udf.formatTimestamp(null, "yyyy-MM-dd");

    // Then:
    assertThat(returnValue, is(nullValue()));
  }

  @Test
  public void shouldReturnNullOnNullDateFormat() {
    // When:
    final String returnValue = udf.formatTimestamp( new Timestamp(1534353043000L), null);

    // Then:
    assertThat(returnValue, is(nullValue()));
  }

  @Test
  public void shouldReturnNullOnNullTimeZone() {
    // When:
    final String returnValue = udf.formatTimestamp( new Timestamp(1534353043000L), "yyyy-MM-dd", null);

    // Then:
    assertThat(returnValue, is(nullValue()));
  }

  @Test
  public void shouldThrowIfInvalidTimeZone() {
    // When:
    final KsqlException e = assertThrows(
        KsqlFunctionException.class,
        () -> udf.formatTimestamp(new Timestamp(1638360611123L),
            "yyyy-MM-dd HH:mm:ss.SSS", "PST")
    );

    // Then:
    assertThat(e.getMessage(), containsString("Unknown time-zone ID: PST"));
  }

  @Test
  public void shouldSupportEmbeddedChars() {
    // When:
    final Object result = udf.formatTimestamp(new Timestamp(1638360611123L),
        "yyyy-MM-dd'T'HH:mm:ss.SSS'Fred'");

    // Then:
    final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Fred'");
    sdf.setTimeZone(TimeZone.getTimeZone("GMT"));
    final String expectedResult = sdf.format(new Date(1638360611123L));
    assertThat(result, is(expectedResult));
  }

  @Test
  public void shoudlReturnNull() {
    // When:
    final Object result = udf.formatTimestamp(null, "yyyy-MM-dd'T'HH:mm:ss.SSS");

    // Then:
    assertNull(result);
  }

  @Test
  public void shouldThrowIfFormatInvalid() {
    // When:
    final KsqlException e = assertThrows(
        KsqlFunctionException.class,
        () -> udf.formatTimestamp(new Timestamp(1638360611123L), "invalid")
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
          udf.formatTimestamp(new Timestamp(1538361611123L), "yyyy-MM-dd HH:mm:ss.SSS");
        });
  }

  @Test
  public void shouldWorkWithManyDifferentFormatters() {
    IntStream.range(0, 10_000)
        .parallel()
        .forEach(idx -> {
          try {
            final String pattern = "yyyy-MM-dd HH:mm:ss.SSS'X" + idx + "'";
            final Timestamp timestamp = new Timestamp(1538361611123L + idx);
            final String result = udf.formatTimestamp(timestamp, pattern);
            final SimpleDateFormat sdf = new SimpleDateFormat(pattern);
            sdf.setTimeZone(TimeZone.getTimeZone("GMT"));
            final String expectedResult = sdf.format(timestamp);
            assertThat(result, is(expectedResult));
          } catch (final Exception e) {
            fail(e.getMessage());
          }
        });
  }

  @Test
  public void shouldRoundTripWithParseTimestamp() {
    final String pattern = "yyyy-MM-dd HH:mm:ss.SSS'Freya'";
    final ParseTimestamp parseTimestamp = new ParseTimestamp();
    IntStream.range(-10_000, 20_000)
        .parallel()
        .forEach(idx -> {
          final Timestamp timestamp = new Timestamp(1538361611123L + idx);
          final String result = udf.formatTimestamp(timestamp, pattern);
          final SimpleDateFormat sdf = new SimpleDateFormat(pattern);
          sdf.setTimeZone(TimeZone.getTimeZone("GMT"));
          final String expectedResult = sdf.format(timestamp);
          assertThat(result, is(expectedResult));

          final Timestamp roundtripTimestamp = parseTimestamp.parseTimestamp(result, pattern);
          assertThat(roundtripTimestamp, is(timestamp));
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
    final SimpleDateFormat sdf = new SimpleDateFormat(format);
    sdf.setTimeZone(TimeZone.getTimeZone("GMT"));
    final String expected = sdf.format(1538361611123L);
    final Object result = new FormatTimestamp()
        .formatTimestamp(new Timestamp(1538361611123L), format);
    assertThat(result, is(expected));
  }
}