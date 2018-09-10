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

import io.confluent.ksql.function.KsqlFunctionException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.stream.IntStream;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class TimestampToStringTest {

  private TimestampToString udf;

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Before
  public void setUp(){
    udf = new TimestampToString();
  }

  @Test
  public void shouldCovertTimestampToString() {
    // When:
    final Object result = udf.evaluate(1638360611123L, "yyyy-MM-dd HH:mm:ss.SSS");

    // Then:
    final String expectedResult = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
        .format(new Date(1638360611123L));
    assertThat(result, is(expectedResult));
  }

  @Test
  public void testUTCTimeZone() {
    // When:
    final Object result = udf.evaluate(1534353043000L, "yyyy-MM-dd HH:mm:ss", "UTC");

    // Then:
    assertThat(result, is("2018-08-15 17:10:43"));
  }

  @Test
  public void testPSTTimeZone() {
    // When:
    final Object result = udf.evaluate(1534353043000L,
        "yyyy-MM-dd HH:mm:ss", "America/Los_Angeles");

    // Then:
    assertThat(result, is("2018-08-15 10:10:43"));
  }

  @Test
  public void testTimeZoneInFormat() {
    // When:
    final long timestamp = 1534353043000L;
    final Object localTime = udf.evaluate(timestamp,
        "yyyy-MM-dd HH:mm:ss zz");
    final Object pacificTime = udf.evaluate(timestamp,
        "yyyy-MM-dd HH:mm:ss zz", "America/Los_Angeles");
    final Object universalTime = udf.evaluate(timestamp,
        "yyyy-MM-dd HH:mm:ss zz", "UTC");

    // Then:
    final String expected = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss zz")
        .format(new Date(timestamp));

    assertThat(localTime, is(expected));
    assertThat(pacificTime, is("2018-08-15 10:10:43 PDT"));
    assertThat(universalTime, is("2018-08-15 17:10:43 UTC"));
  }

  @Test
  public void shouldThrowIfInvalidTimeZone() {
    expectedException.expect(KsqlFunctionException.class);
    expectedException.expectMessage("Unknown time-zone ID: PST");
    udf.evaluate(1638360611123L, "yyyy-MM-dd HH:mm:ss.SSS", "PST");
  }

  @Test
  public void shouldSupportEmbeddedChars() {
    // When:
    final Object result = udf.evaluate(1638360611123L, "yyyy-MM-dd'T'HH:mm:ss.SSS'Fred'");

    // Then:
    final String expectedResult = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Fred'")
        .format(new Date(1638360611123L));
    assertThat(result, is(expectedResult));
  }

  @Test
  public void shouldThrowIfTooFewParameters() {
    expectedException.expect(KsqlFunctionException.class);
    expectedException.expectMessage(
        "should have at least two input arguments: date value and format.");
    udf.evaluate(1638360611123L);
  }

  @Test
  public void shouldThrowIfTooManyParameters() {
    expectedException.expect(KsqlFunctionException.class);
    expectedException.expectMessage(
        "should have at most three input arguments: date value, format and zone.");
    udf.evaluate(1638360611123L, "yyyy-MM-dd HH:mm:ss.SSS", "UTC", "extra");
  }

  @Test
  public void shouldThrowIfFormatInvalid() {
    expectedException.expect(KsqlFunctionException.class);
    expectedException.expectMessage("Unknown pattern letter: i");
    udf.evaluate("2021-12-01 12:10:11.123", "invalid");
  }

  @Test
  public void shouldThrowIfNotTimestamp() {
    expectedException.expect(KsqlFunctionException.class);
    expectedException.expectMessage("java.lang.String cannot be cast to java.lang.Long");
    udf.evaluate("invalid", "2021-12-01 12:10:11.123");
  }

  @Test
  public void shouldBeThreadSafe() {
    IntStream.range(0, 10_000)
        .parallel()
        .forEach(idx -> {
          shouldCovertTimestampToString();
          udf.evaluate(1538361611123L, "yyyy-MM-dd HH:mm:ss.SSS");
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
    final Object result = new TimestampToString().evaluate(1538361611123L, format);
    assertThat(result, is(expected));
  }
}
