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

import org.junit.Before;
import org.junit.Test;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.stream.IntStream;

import io.confluent.ksql.function.KsqlFunctionException;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class TimestampToStringTest {

  private TimestampToString udf;

  @Before
  public void setUp(){
    udf = new TimestampToString();
  }

  @Test
  public void shouldCovertTimestampToString() {
    // When:
    final Object result = udf.evaluate(1638360611123L, "yyyy-MM-dd HH:mm:ss.SSS");

    // Then:
    String expectedResult = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
        .format(new Date(1638360611123L));
    assertThat(result, is(expectedResult));
  }

  @Test
  public void shouldSupportEmbeddedChars() {
    // When:
    final Object result = udf.evaluate(1638360611123L, "yyyy-MM-dd'T'HH:mm:ss.SSS'Fred'");

    // Then:
    String expectedResult = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Fred'")
        .format(new Date(1638360611123L));
    assertThat(result, is(expectedResult));
  }

  @Test(expected = KsqlFunctionException.class)
  public void shouldThrowIfTooFewParameters() {
    udf.evaluate(1638360611123L);
  }

  @Test(expected = KsqlFunctionException.class)
  public void shouldThrowIfTooManyParameters() {
    udf.evaluate(1638360611123L, "yyyy-MM-dd HH:mm:ss.SSS", "extra");
  }

  @Test(expected = KsqlFunctionException.class)
  public void shouldThrowIfFormatInvalid() {
    udf.evaluate("2021-12-01 12:10:11.123", "invalid");
  }

  @Test(expected = KsqlFunctionException.class)
  public void shouldThrowIfNotTimestamp() {
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