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

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.stream.IntStream;

import io.confluent.ksql.function.KsqlFunctionException;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class StringToTimestampTest {

  private StringToTimestamp udf;

  @Before
  public void setUp(){
    udf = new StringToTimestamp();
  }

  @Test
  public void shouldCovertStringToTimestamp() throws ParseException {
    // When:
    final Object result = udf.evaluate("2021-12-01 12:10:11.123", "yyyy-MM-dd HH:mm:ss.SSS");

    // Then:
    long expectedResult = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
        .parse("2021-12-01 12:10:11.123").getTime();
    assertThat(result, is(expectedResult));
  }

  @Test
  public void shouldSupportEmbeddedChars() throws ParseException {
    // When:
    final Object result = udf.evaluate("2021-12-01T12:10:11.123Fred", "yyyy-MM-dd'T'HH:mm:ss.SSS'Fred'");

    // Then:
    long expectedResult = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Fred'")
        .parse("2021-12-01T12:10:11.123Fred").getTime();
    assertThat(result, is(expectedResult));
  }

  @Test(expected = KsqlFunctionException.class)
  public void shouldThrowIfTooFewParameters() {
    udf.evaluate("2021-12-01 12:10:11.123");
  }

  @Test(expected = KsqlFunctionException.class)
  public void shouldThrowIfTooManyParameters() {
    udf.evaluate("2021-12-01 12:10:11.123", "yyyy-MM-dd HH:mm:ss.SSS", "extra");
  }

  @Test(expected = KsqlFunctionException.class)
  public void shouldThrowIfFormatInvalid() {
    udf.evaluate("2021-12-01 12:10:11.123", "invalid");
  }

  @Test(expected = KsqlFunctionException.class)
  public void shouldThrowIfParseFails() {
    udf.evaluate("invalid", "yyyy-MM-dd'T'HH:mm:ss.SSS");
  }

  @Test(expected = KsqlFunctionException.class)
  public void shouldThrowOnEmptyString() {
    udf.evaluate("invalid", "yyyy-MM-dd'T'HH:mm:ss.SSS");
  }

  @Test
  public void shouldBeThreadSafe() {
    IntStream.range(0, 10_000)
        .parallel()
        .forEach(idx -> {
          try {
            shouldCovertStringToTimestamp();
          } catch (ParseException e) {
            Assert.fail(e.getMessage());
          }
          udf.evaluate("1988-01-12 10:12:13.456", "yyyy-MM-dd HH:mm:ss.SSS");
        });
  }

  @Test
  public void shouldBehaveLikeSimpleDateFormat() throws Exception {
    assertLikeSimpleDateFormat("2021-12-01 12:10:11.123", "yyyy-MM-dd HH:mm:ss.SSS");
    assertLikeSimpleDateFormat("2021-12-01 12:10:11.123-0800", "yyyy-MM-dd HH:mm:ss.SSSX");
    assertLikeSimpleDateFormat("2021-12-01 12:10:11.123 +00", "yyyy-MM-dd HH:mm:ss.SSS X");
    assertLikeSimpleDateFormat("2021-12-01 12:10:11.123 PST", "yyyy-MM-dd HH:mm:ss.SSS z");
    assertLikeSimpleDateFormat("2021-12-01 12:10:11.123 -0800", "yyyy-MM-dd HH:mm:ss.SSS Z");
    assertLikeSimpleDateFormat("2021-12-01 12:10:11", "yyyy-MM-dd HH:mm:ss");
    assertLikeSimpleDateFormat("2021-12-01 12:10:11 -0700", "yyyy-MM-dd HH:mm:ss X");
    assertLikeSimpleDateFormat("2021-12-01 12:10", "yyyy-MM-dd HH:mm");
    assertLikeSimpleDateFormat("2021-12-01 12:10 -0700", "yyyy-MM-dd HH:mm X");
    assertLikeSimpleDateFormat("2021-12-01 12", "yyyy-MM-dd HH");
    assertLikeSimpleDateFormat("2021-12-01 12 -0700", "yyyy-MM-dd HH X");
    assertLikeSimpleDateFormat("2021-12-01", "yyyy-MM-dd");
    assertLikeSimpleDateFormat("2021-12-01 PST", "yyyy-MM-dd z");
    assertLikeSimpleDateFormat("2021-12", "yyyy-MM");
    assertLikeSimpleDateFormat("2021", "yyyy");
    assertLikeSimpleDateFormat("12", "MM");
    assertLikeSimpleDateFormat("01", "dd");
    assertLikeSimpleDateFormat("01", "HH");
    assertLikeSimpleDateFormat("01", "mm");
  }

  private void assertLikeSimpleDateFormat(final String value, final String format) throws Exception {
    final long expected = new SimpleDateFormat(format).parse(value).getTime();
    final Object result = new StringToTimestamp().evaluate(value, format);
    assertThat(result, is(expected));
  }
}