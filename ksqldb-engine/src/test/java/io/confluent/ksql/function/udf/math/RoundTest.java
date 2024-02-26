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

package io.confluent.ksql.function.udf.math;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import java.math.BigDecimal;
import org.junit.Before;
import org.junit.Test;

public class RoundTest {

  private Round udf;

  @Before
  public void setUp() {
    udf = new Round();
  }

  @Test
  public void shouldRoundSimpleDoublePositive() {
    assertThat(udf.round(0.0d), is(0L));
    assertThat(udf.round(1.23d), is(1L));
    assertThat(udf.round(1.0d), is(1L));
    assertThat(udf.round(1.5d), is(2L));
    assertThat(udf.round(1.75d), is(2L));
    assertThat(udf.round(1.53e6d), is(1530000L));
    assertThat(udf.round(10.01d), is(10L));
    assertThat(udf.round(12345.5d), is(12346L));
    assertThat(udf.round(9.99d), is(10L));
    assertThat(udf.round(110.1), is(110L));
    assertThat(udf.round(1530000.01d), is(1530000L));
    assertThat(udf.round(9999999.99d), is(10000000L));
  }

  @Test
  public void shouldRoundSimpleDoubleNegative() {
    assertThat(udf.round(-1.23d), is(-1L));
    assertThat(udf.round(-1.0d), is(-1L));
    assertThat(udf.round(-1.5d), is(-1L));
    assertThat(udf.round(-1.75d), is(-2L));
    assertThat(udf.round(-1.53e6d), is(-1530000L));
    assertThat(udf.round(-10.01d), is(-10L));
    assertThat(udf.round(-12345.5d), is(-12345L));
    assertThat(udf.round(-9.99d), is(-10L));
    assertThat(udf.round(-110.1), is(-110L));
    assertThat(udf.round(-1530000.01d), is(-1530000L));
    assertThat(udf.round(-9999999.99d), is(-10000000L));
  }

  @Test
  public void shouldRoundSimpleBigDecimalPositive() {
    assertThat(udf.round(new BigDecimal("0.0")), is(new BigDecimal("0")));
    assertThat(udf.round(new BigDecimal("1.23")), is(new BigDecimal("1")));
    assertThat(udf.round(new BigDecimal("1.0")), is(new BigDecimal("1")));
    assertThat(udf.round(new BigDecimal("1.5")), is(new BigDecimal("2")));
    assertThat(udf.round(new BigDecimal("1.75")), is(new BigDecimal("2")));
    assertThat(udf.round(new BigDecimal("1530000")), is(new BigDecimal("1530000")));
    assertThat(udf.round(new BigDecimal("10.1")), is(new BigDecimal("10")));
    assertThat(udf.round(new BigDecimal("12345.5")), is(new BigDecimal("12346")));
    assertThat(udf.round(new BigDecimal("9.99")), is(new BigDecimal("10")));
    assertThat(udf.round(new BigDecimal("110.1")), is(new BigDecimal("110")));
    assertThat(udf.round(new BigDecimal("1530000.01")), is(new BigDecimal("1530000")));
    assertThat(udf.round(new BigDecimal("9999999.99")), is(new BigDecimal("10000000")));
  }

  @Test
  public void shouldRoundSimpleBigDecimalNegative() {
    assertThat(udf.round(new BigDecimal("-1.23")), is(new BigDecimal("-1")));
    assertThat(udf.round(new BigDecimal("-1.0")), is(new BigDecimal("-1")));
    assertThat(udf.round(new BigDecimal("-1.5")), is(new BigDecimal("-1")));
    assertThat(udf.round(new BigDecimal("-1530000")), is(new BigDecimal("-1530000")));
    assertThat(udf.round(new BigDecimal("-10.1")), is(new BigDecimal("-10")));
    assertThat(udf.round(new BigDecimal("-12345.5")), is(new BigDecimal("-12345")));
    assertThat(udf.round(new BigDecimal("-9.99")), is(new BigDecimal("-10")));
    assertThat(udf.round(new BigDecimal("-110.1")), is(new BigDecimal("-110")));
    assertThat(udf.round(new BigDecimal("-1530000.01")), is(new BigDecimal("-1530000")));
    assertThat(udf.round(new BigDecimal("-9999999.99")), is(new BigDecimal("-10000000")));
  }

  @Test
  public void shouldRoundDoubleWithDecimalPlacesPositive() {
    assertThat(udf.round(0d, 0), is(0d));
    assertThat(udf.round(1.0d, 0), is(1.0d));
    assertThat(udf.round(1.1d, 0), is(1.0d));
    assertThat(udf.round(1.5d, 0), is(2.0d));
    assertThat(udf.round(1.75d, 0), is(2.0d));
    assertThat(udf.round(100.1d, 0), is(100.0d));
    assertThat(udf.round(100.5d, 0), is(101.0d));
    assertThat(udf.round(100.75d, 0), is(101.0d));
    assertThat(udf.round(100.10d, 1), is(100.1d));
    assertThat(udf.round(100.11d, 1), is(100.1d));
    assertThat(udf.round(100.15d, 1), is(100.2d));
    assertThat(udf.round(100.17d, 1), is(100.2d));
    assertThat(udf.round(100.110d, 2), is(100.11d));
    assertThat(udf.round(100.111d, 2), is(100.11d));
    assertThat(udf.round(100.115d, 2), is(100.12d));
    assertThat(udf.round(100.117d, 2), is(100.12d));
    assertThat(udf.round(100.1110d, 3), is(100.111d));
    assertThat(udf.round(100.1111d, 3), is(100.111d));
    assertThat(udf.round(100.1115d, 3), is(100.112d));
    assertThat(udf.round(100.1117d, 3), is(100.112d));
    assertThat(udf.round(12345.67d, -1), is(12350d));
    assertThat(udf.round(12345.67d, -2), is(12300d));
    assertThat(udf.round(12345.67d, -3), is(12000d));
    assertThat(udf.round(12345.67d, -4), is(10000d));
    assertThat(udf.round(12345.67d, -5), is(0d));
  }

  @Test
  public void shouldRoundDoubleWithDecimalPlacesNegative() {
    assertThat(udf.round(-1.0d, 0), is(-1.0d));
    assertThat(udf.round(-1.1d, 0), is(-1.0d));
    assertThat(udf.round(-1.5d, 0), is(-1.0d));
    assertThat(udf.round(-1.75d, 0), is(-2.0d));
    assertThat(udf.round(-100.1d, 0), is(-100.0d));
    assertThat(udf.round(-100.5d, 0), is(-100.0d));
    assertThat(udf.round(-100.75d, 0), is(-101.0d));
    assertThat(udf.round(-100.10d, 1), is(-100.1d));
    assertThat(udf.round(-100.11d, 1), is(-100.1d));
    assertThat(udf.round(-100.15d, 1), is(-100.1d));
    assertThat(udf.round(-100.17d, 1), is(-100.2d));
    assertThat(udf.round(-100.110d, 2), is(-100.11d));
    assertThat(udf.round(-100.111d, 2), is(-100.11d));
    assertThat(udf.round(-100.115d, 2), is(-100.11d));
    assertThat(udf.round(-100.117d, 2), is(-100.12d));
    assertThat(udf.round(-100.1110d, 3), is(-100.111d));
    assertThat(udf.round(-100.1111d, 3), is(-100.111d));
    assertThat(udf.round(-100.1115d, 3), is(-100.111d));
    assertThat(udf.round(-100.1117d, 3), is(-100.112d));
    assertThat(udf.round(-12345.67d, -1), is(-12350d));
    assertThat(udf.round(-12345.67d, -2), is(-12300d));
    assertThat(udf.round(-12345.67d, -3), is(-12000d));
    assertThat(udf.round(-12345.67d, -4), is(-10000d));
    assertThat(udf.round(-12345.67d, -5), is(0d));
  }

  @Test
  public void shouldRoundBigDecimalWithDecimalPlacesPositive() {
    assertThat(udf.round(new BigDecimal("0"), 0), is(new BigDecimal("0")));
    assertThat(udf.round(new BigDecimal("1.0"), 0), is(new BigDecimal("1.0")));
    assertThat(udf.round(new BigDecimal("1.1"), 0), is(new BigDecimal("1.0")));
    assertThat(udf.round(new BigDecimal("1.5"), 0), is(new BigDecimal("2.0")));
    assertThat(udf.round(new BigDecimal("1.75"), 0), is(new BigDecimal("2.00")));
    assertThat(udf.round(new BigDecimal("100.1"), 0),is(new BigDecimal("100.0")));
    assertThat(udf.round(new BigDecimal("100.5"), 0), is(new BigDecimal("101.0")));
    assertThat(udf.round(new BigDecimal("100.75"), 0), is(new BigDecimal("101.00")));
    assertThat(udf.round(new BigDecimal("100.10"), 1), is(new BigDecimal("100.10")));
    assertThat(udf.round(new BigDecimal("100.11"), 1), is(new BigDecimal("100.10")));
    assertThat(udf.round(new BigDecimal("100.15"), 1), is(new BigDecimal("100.20")));
    assertThat(udf.round(new BigDecimal("100.17"), 1), is(new BigDecimal("100.20")));
    assertThat(udf.round(new BigDecimal("100.110"), 2), is(new BigDecimal("100.110")));
    assertThat(udf.round(new BigDecimal("100.111"), 2), is(new BigDecimal("100.110")));
    assertThat(udf.round(new BigDecimal("100.115"), 2), is(new BigDecimal("100.120")));
    assertThat(udf.round(new BigDecimal("100.117"), 2), is(new BigDecimal("100.120")));
    assertThat(udf.round(new BigDecimal("100.1110"), 3), is(new BigDecimal("100.1110")));
    assertThat(udf.round(new BigDecimal("100.1111"), 3), is(new BigDecimal("100.1110")));
    assertThat(udf.round(new BigDecimal("100.1115"), 3), is(new BigDecimal("100.1120")));
    assertThat(udf.round(new BigDecimal("100.1117"), 3), is(new BigDecimal("100.1120")));
    assertThat(udf.round(new BigDecimal("12345.67"), -1), is(new BigDecimal("12350.00")));
    assertThat(udf.round(new BigDecimal("12345.67"), -2), is(new BigDecimal("12300.00")));
    assertThat(udf.round(new BigDecimal("12345.67"), -3), is(new BigDecimal("12000.00")));
    assertThat(udf.round(new BigDecimal("12345.67"), -4), is(new BigDecimal("10000.00")));
    assertThat(udf.round(new BigDecimal("12345.67"), -5), is(new BigDecimal("0.00")));
  }

  @Test
  public void shouldRoundBigDecimalWithDecimalPlacesNegative() {
    assertThat(udf.round(new BigDecimal("-1.0"), 0), is(new BigDecimal("-1.0")));
    assertThat(udf.round(new BigDecimal("-1.1"), 0), is(new BigDecimal("-1.0")));
    assertThat(udf.round(new BigDecimal("-1.5"), 0), is(new BigDecimal("-1.0")));
    assertThat(udf.round(new BigDecimal("-1.75"), 0), is(new BigDecimal("-2.00")));
    assertThat(udf.round(new BigDecimal("-100.1"), 0), is(new BigDecimal("-100.0")));
    assertThat(udf.round(new BigDecimal("-100.5"), 0), is(new BigDecimal("-100.0")));
    assertThat(udf.round(new BigDecimal("-100.75"), 0), is(new BigDecimal("-101.00")));
    assertThat(udf.round(new BigDecimal("-100.10"), 1), is(new BigDecimal("-100.10")));
    assertThat(udf.round(new BigDecimal("-100.11"), 1), is(new BigDecimal("-100.10")));
    assertThat(udf.round(new BigDecimal("-100.15"), 1), is(new BigDecimal("-100.10")));
    assertThat(udf.round(new BigDecimal("-100.17"), 1), is(new BigDecimal("-100.20")));
    assertThat(udf.round(new BigDecimal("-100.110"), 2), is(new BigDecimal("-100.110")));
    assertThat(udf.round(new BigDecimal("-100.111"), 2), is(new BigDecimal("-100.110")));
    assertThat(udf.round(new BigDecimal("-100.115"), 2), is(new BigDecimal("-100.110")));
    assertThat(udf.round(new BigDecimal("-100.117"), 2), is(new BigDecimal("-100.120")));
    assertThat(udf.round(new BigDecimal("-100.1110"), 3), is(new BigDecimal("-100.1110")));
    assertThat(udf.round(new BigDecimal("-100.1111"), 3), is(new BigDecimal("-100.1110")));
    assertThat(udf.round(new BigDecimal("-100.1115"), 3), is(new BigDecimal("-100.1110")));
    assertThat(udf.round(new BigDecimal("-100.1117"), 3), is(new BigDecimal("-100.1120")));
    assertThat(udf.round(new BigDecimal("-12345.67"), -2), is(new BigDecimal("-12300.00")));
    assertThat(udf.round(new BigDecimal("-12345.67"), -3), is(new BigDecimal("-12000.00")));
    assertThat(udf.round(new BigDecimal("-12345.67"), -4), is(new BigDecimal("-10000.00")));
    assertThat(udf.round(new BigDecimal("-12345.67"), -5), is(new BigDecimal("0.00")));
  }

  @Test
  public void shouldHandleDoubleLiteralsEndingWith5ThatCannotBeRepresentedExactlyAsDoubles() {
    assertThat(udf.round(new BigDecimal("265.335"), 2), is(new BigDecimal("265.340")));
    assertThat(udf.round(new BigDecimal("-265.335"), 2), is(new BigDecimal("-265.330")));

    assertThat(udf.round(new BigDecimal("265.365"), 2), is(new BigDecimal("265.370")));
    assertThat(udf.round(new BigDecimal("-265.365"), 2), is(new BigDecimal("-265.360")));
  }

  @Test
  public void shoulldHandleNullValues() {
    assertThat(udf.round((Double)null), is((Long)null));
    assertThat(udf.round((BigDecimal) null), is((BigDecimal) null));
    assertThat(udf.round((Double)null, 2), is((Long)null));
    assertThat(udf.round((BigDecimal) null, 2), is((BigDecimal) null));
  }

  @Test
  public void shoulldHandleNullDecimalPlaces() {
    assertThat(udf.round(1.75d, null), is(nullValue()));
    assertThat(udf.round(new BigDecimal("1.75"), null), is(nullValue()));
  }


  @Test
  public void shouldRoundInt() {
    assertThat(udf.round(123), is(123L));
  }

  @Test
  public void shouldRoundLong() {
    assertThat(udf.round(123L), is(123L));
  }

}