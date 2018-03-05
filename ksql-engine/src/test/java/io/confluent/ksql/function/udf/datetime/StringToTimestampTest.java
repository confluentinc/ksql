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
  public void shouldCovertStringToTimestamp() {
    // When:
    final Object result = udf.evaluate("2021-12-01 12:10:11.123", "yyyy-MM-dd HH:mm:ss.SSS");

    // Then:
    assertThat(result, is(1638360611123L));
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

  @Test
  public void shouldBeThreadSafe() {
    IntStream.range(0, 10_000)
        .parallel()
        .forEach(idx -> shouldCovertStringToTimestamp());
  }
}