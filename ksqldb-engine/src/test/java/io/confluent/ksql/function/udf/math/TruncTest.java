/*
 * Copyright 2022 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.function.udf.math;

import org.junit.Before;
import org.junit.Test;
import java.math.BigDecimal;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class TruncTest {
  private Trunc udf;

  @Before
  public void setUp() {
    udf = new Trunc();
  }

  @Test
  public void shouldTruncateSimpleDoublePositive() {
    assertThat(udf.trunc(0.0d), is(0L));
    assertThat(udf.trunc(1.23d), is(1L));
    assertThat(udf.trunc(1.0d), is(1L));
    assertThat(udf.trunc(1.5d), is(1L));
    assertThat(udf.trunc(1.75d), is(1L));
    assertThat(udf.trunc(1.53e6d), is(1530000L));
    assertThat(udf.trunc(10.01d), is(10L));
    assertThat(udf.trunc(12345.5d), is(12345L));
    assertThat(udf.trunc(9.99d), is(9L));
    assertThat(udf.trunc(110.1), is(110L));
    assertThat(udf.trunc(1530000.01d), is(1530000L));
    assertThat(udf.trunc(9999999.99d), is(9999999L));
  }

  @Test
  public void shouldTruncateSimpleDoubleNegative() {
    assertThat(udf.trunc(-1.23d), is(-1L));
    assertThat(udf.trunc(-1.0d), is(-1L));
    assertThat(udf.trunc(-1.5d), is(-1L));
    assertThat(udf.trunc(-1.75d), is(-1L));
    assertThat(udf.trunc(-1.53e6d), is(-1530000L));
    assertThat(udf.trunc(-10.01d), is(-10L));
    assertThat(udf.trunc(-12345.5d), is(-12345L));
    assertThat(udf.trunc(-9.99d), is(-9L));
    assertThat(udf.trunc(-110.1), is(-110L));
    assertThat(udf.trunc(-1530000.01d), is(-1530000L));
    assertThat(udf.trunc(-9999999.99d), is(-9999999L));
  }

  @Test
  public void shouldTruncateSimpleBigDecimalPositive() {
    assertThat(udf.trunc(new BigDecimal("0.0")), is(new BigDecimal("0")));
    assertThat(udf.trunc(new BigDecimal("1.23")), is(new BigDecimal("1")));
    assertThat(udf.trunc(new BigDecimal("1.0")), is(new BigDecimal("1")));
    assertThat(udf.trunc(new BigDecimal("1.5")), is(new BigDecimal("1")));
    assertThat(udf.trunc(new BigDecimal("1.75")), is(new BigDecimal("1")));
    assertThat(udf.trunc(new BigDecimal("1530000")), is(new BigDecimal("1530000")));
    assertThat(udf.trunc(new BigDecimal("10.1")), is(new BigDecimal("10")));
    assertThat(udf.trunc(new BigDecimal("12345.5")), is(new BigDecimal("12345")));
    assertThat(udf.trunc(new BigDecimal("9.99")), is(new BigDecimal("9")));
    assertThat(udf.trunc(new BigDecimal("110.1")), is(new BigDecimal("110")));
    assertThat(udf.trunc(new BigDecimal("1530000.01")), is(new BigDecimal("1530000")));
    assertThat(udf.trunc(new BigDecimal("9999999.99")), is(new BigDecimal("9999999")));
  }

  @Test
  public void shouldTruncateSimpleBigDecimalNegative() {
    assertThat(udf.trunc(new BigDecimal("-1.23")), is(new BigDecimal("-1")));
    assertThat(udf.trunc(new BigDecimal("-1.0")), is(new BigDecimal("-1")));
    assertThat(udf.trunc(new BigDecimal("-1.5")), is(new BigDecimal("-1")));
    assertThat(udf.trunc(new BigDecimal("-1530000")), is(new BigDecimal("-1530000")));
    assertThat(udf.trunc(new BigDecimal("-10.1")), is(new BigDecimal("-10")));
    assertThat(udf.trunc(new BigDecimal("-12345.5")), is(new BigDecimal("-12345")));
    assertThat(udf.trunc(new BigDecimal("-9.99")), is(new BigDecimal("-9")));
    assertThat(udf.trunc(new BigDecimal("-110.1")), is(new BigDecimal("-110")));
    assertThat(udf.trunc(new BigDecimal("-1530000.01")), is(new BigDecimal("-1530000")));
    assertThat(udf.trunc(new BigDecimal("-9999999.99")), is(new BigDecimal("-9999999")));
  }

  @Test
  public void shouldTruncateDoubleWithDecimalPlacesPositive() {
    assertThat(udf.trunc(0d, 0), is(0d));
    assertThat(udf.trunc(1.0d, 0), is(1.0d));
    assertThat(udf.trunc(1.1d, 0), is(1.0d));
    assertThat(udf.trunc(1.5d, 0), is(1.0d));
    assertThat(udf.trunc(1.75d, 0), is(1.0d));
    assertThat(udf.trunc(100.1d, 0), is(100.0d));
    assertThat(udf.trunc(100.5d, 0), is(100.0d));
    assertThat(udf.trunc(100.75d, 0), is(100.0d));
    assertThat(udf.trunc(100.10d, 1), is(100.1d));
    assertThat(udf.trunc(100.11d, 1), is(100.1d));
    assertThat(udf.trunc(100.15d, 1), is(100.1d));
    assertThat(udf.trunc(100.17d, 1), is(100.1d));
    assertThat(udf.trunc(100.110d, 2), is(100.11d));
    assertThat(udf.trunc(100.111d, 2), is(100.11d));
    assertThat(udf.trunc(100.115d, 2), is(100.11d));
    assertThat(udf.trunc(100.117d, 2), is(100.11d));
    assertThat(udf.trunc(100.1110d, 3), is(100.111d));
    assertThat(udf.trunc(100.1111d, 3), is(100.111d));
    assertThat(udf.trunc(100.1115d, 3), is(100.111d));
    assertThat(udf.trunc(100.1117d, 3), is(100.111d));
    assertThat(udf.trunc(1.0d, 3), is(1.0d));
    assertThat(udf.trunc(1.1d, 3), is(1.1d));
    assertThat(udf.trunc(1.5d, 3), is(1.5d));
    assertThat(udf.trunc(1.7d, 3), is(1.7d));
    assertThat(udf.trunc(12345.67d, -1), is(12340d));
    assertThat(udf.trunc(12345.67d, -2), is(12300d));
    assertThat(udf.trunc(12345.67d, -3), is(12000d));
    assertThat(udf.trunc(12345.67d, -4), is(10000d));
    assertThat(udf.trunc(12345.67d, -5), is(0d));
  }

  @Test
  public void shouldTruncateDoubleWithDecimalPlacesNegative() {
    assertThat(udf.trunc(-1.0d, 0), is(-1.0d));
    assertThat(udf.trunc(-1.1d, 0), is(-1.0d));
    assertThat(udf.trunc(-1.5d, 0), is(-1.0d));
    assertThat(udf.trunc(-1.75d, 0), is(-1.0d));
    assertThat(udf.trunc(-100.1d, 0), is(-100.0d));
    assertThat(udf.trunc(-100.5d, 0), is(-100.0d));
    assertThat(udf.trunc(-100.75d, 0), is(-100.0d));
    assertThat(udf.trunc(-100.10d, 1), is(-100.1d));
    assertThat(udf.trunc(-100.11d, 1), is(-100.1d));
    assertThat(udf.trunc(-100.15d, 1), is(-100.1d));
    assertThat(udf.trunc(-100.17d, 1), is(-100.1d));
    assertThat(udf.trunc(-100.110d, 2), is(-100.11d));
    assertThat(udf.trunc(-100.111d, 2), is(-100.11d));
    assertThat(udf.trunc(-100.115d, 2), is(-100.11d));
    assertThat(udf.trunc(-100.117d, 2), is(-100.11d));
    assertThat(udf.trunc(-100.1110d, 3), is(-100.111d));
    assertThat(udf.trunc(-100.1111d, 3), is(-100.111d));
    assertThat(udf.trunc(-100.1115d, 3), is(-100.111d));
    assertThat(udf.trunc(-100.1117d, 3), is(-100.111d));
    assertThat(udf.trunc(-1.0d, 3), is(-1.0d));
    assertThat(udf.trunc(-1.1d, 3), is(-1.1d));
    assertThat(udf.trunc(-1.5d, 3), is(-1.5d));
    assertThat(udf.trunc(-1.7d, 3), is(-1.7d));
    assertThat(udf.trunc(-12345.67d, -1), is(-12340d));
    assertThat(udf.trunc(-12345.67d, -2), is(-12300d));
    assertThat(udf.trunc(-12345.67d, -3), is(-12000d));
    assertThat(udf.trunc(-12345.67d, -4), is(-10000d));
    assertThat(udf.trunc(-12345.67d, -5), is(0d));
  }

  @Test
  public void shouldTruncateBigDecimalWithDecimalPlacesPositive() {
    assertThat(udf.trunc(new BigDecimal("0"), 0), is(new BigDecimal("0")));
    assertThat(udf.trunc(new BigDecimal("1.0"), 0), is(new BigDecimal("1.0")));
    assertThat(udf.trunc(new BigDecimal("1.1"), 0), is(new BigDecimal("1.0")));
    assertThat(udf.trunc(new BigDecimal("1.5"), 0), is(new BigDecimal("1.0")));
    assertThat(udf.trunc(new BigDecimal("1.75"), 0), is(new BigDecimal("1.00")));
    assertThat(udf.trunc(new BigDecimal("100.1"), 0),is(new BigDecimal("100.0")));
    assertThat(udf.trunc(new BigDecimal("100.5"), 0), is(new BigDecimal("100.0")));
    assertThat(udf.trunc(new BigDecimal("100.75"), 0), is(new BigDecimal("100.00")));
    assertThat(udf.trunc(new BigDecimal("100.10"), 1), is(new BigDecimal("100.10")));
    assertThat(udf.trunc(new BigDecimal("100.11"), 1), is(new BigDecimal("100.10")));
    assertThat(udf.trunc(new BigDecimal("100.15"), 1), is(new BigDecimal("100.10")));
    assertThat(udf.trunc(new BigDecimal("100.17"), 1), is(new BigDecimal("100.10")));
    assertThat(udf.trunc(new BigDecimal("100.110"), 2), is(new BigDecimal("100.110")));
    assertThat(udf.trunc(new BigDecimal("100.111"), 2), is(new BigDecimal("100.110")));
    assertThat(udf.trunc(new BigDecimal("100.115"), 2), is(new BigDecimal("100.110")));
    assertThat(udf.trunc(new BigDecimal("100.117"), 2), is(new BigDecimal("100.110")));
    assertThat(udf.trunc(new BigDecimal("100.1110"), 3), is(new BigDecimal("100.1110")));
    assertThat(udf.trunc(new BigDecimal("100.1111"), 3), is(new BigDecimal("100.1110")));
    assertThat(udf.trunc(new BigDecimal("100.1115"), 3), is(new BigDecimal("100.1110")));
    assertThat(udf.trunc(new BigDecimal("100.1117"), 3), is(new BigDecimal("100.1110")));
    assertThat(udf.trunc(new BigDecimal("1.0"), 3), is(new BigDecimal("1.0")));
    assertThat(udf.trunc(new BigDecimal("1.1"), 3), is(new BigDecimal("1.1")));
    assertThat(udf.trunc(new BigDecimal("1.5"), 3), is(new BigDecimal("1.5")));
    assertThat(udf.trunc(new BigDecimal("1.7"), 3), is(new BigDecimal("1.7")));
    assertThat(udf.trunc(new BigDecimal("12345.67"), -1), is(new BigDecimal("12340.00")));
    assertThat(udf.trunc(new BigDecimal("12345.67"), -2), is(new BigDecimal("12300.00")));
    assertThat(udf.trunc(new BigDecimal("12345.67"), -3), is(new BigDecimal("12000.00")));
    assertThat(udf.trunc(new BigDecimal("12345.67"), -4), is(new BigDecimal("10000.00")));
    assertThat(udf.trunc(new BigDecimal("12345.67"), -5), is(new BigDecimal("0.00")));
  }

  @Test
  public void shouldTruncateBigDecimalWithDecimalPlacesNegative() {
    assertThat(udf.trunc(new BigDecimal("-1.0"), 0), is(new BigDecimal("-1.0")));
    assertThat(udf.trunc(new BigDecimal("-1.1"), 0), is(new BigDecimal("-1.0")));
    assertThat(udf.trunc(new BigDecimal("-1.5"), 0), is(new BigDecimal("-1.0")));
    assertThat(udf.trunc(new BigDecimal("-1.75"), 0), is(new BigDecimal("-1.00")));
    assertThat(udf.trunc(new BigDecimal("-100.1"), 0), is(new BigDecimal("-100.0")));
    assertThat(udf.trunc(new BigDecimal("-100.5"), 0), is(new BigDecimal("-100.0")));
    assertThat(udf.trunc(new BigDecimal("-100.75"), 0), is(new BigDecimal("-100.00")));
    assertThat(udf.trunc(new BigDecimal("-100.10"), 1), is(new BigDecimal("-100.10")));
    assertThat(udf.trunc(new BigDecimal("-100.11"), 1), is(new BigDecimal("-100.10")));
    assertThat(udf.trunc(new BigDecimal("-100.15"), 1), is(new BigDecimal("-100.10")));
    assertThat(udf.trunc(new BigDecimal("-100.17"), 1), is(new BigDecimal("-100.10")));
    assertThat(udf.trunc(new BigDecimal("-100.110"), 2), is(new BigDecimal("-100.110")));
    assertThat(udf.trunc(new BigDecimal("-100.111"), 2), is(new BigDecimal("-100.110")));
    assertThat(udf.trunc(new BigDecimal("-100.115"), 2), is(new BigDecimal("-100.110")));
    assertThat(udf.trunc(new BigDecimal("-100.117"), 2), is(new BigDecimal("-100.110")));
    assertThat(udf.trunc(new BigDecimal("-100.1110"), 3), is(new BigDecimal("-100.1110")));
    assertThat(udf.trunc(new BigDecimal("-100.1111"), 3), is(new BigDecimal("-100.1110")));
    assertThat(udf.trunc(new BigDecimal("-100.1115"), 3), is(new BigDecimal("-100.1110")));
    assertThat(udf.trunc(new BigDecimal("-100.1117"), 3), is(new BigDecimal("-100.1110")));
    assertThat(udf.trunc(new BigDecimal("-1.0"), 3), is(new BigDecimal("-1.0")));
    assertThat(udf.trunc(new BigDecimal("-1.1"), 3), is(new BigDecimal("-1.1")));
    assertThat(udf.trunc(new BigDecimal("-1.5"), 3), is(new BigDecimal("-1.5")));
    assertThat(udf.trunc(new BigDecimal("-1.7"), 3), is(new BigDecimal("-1.7")));
    assertThat(udf.trunc(new BigDecimal("-12345.67"), -2), is(new BigDecimal("-12300.00")));
    assertThat(udf.trunc(new BigDecimal("-12345.67"), -3), is(new BigDecimal("-12000.00")));
    assertThat(udf.trunc(new BigDecimal("-12345.67"), -4), is(new BigDecimal("-10000.00")));
    assertThat(udf.trunc(new BigDecimal("-12345.67"), -5), is(new BigDecimal("0.00")));
  }

  @Test
  public void shouldHandleDoubleLiteralsEndingWith5ThatCannotBeRepresentedExactlyAsDoubles() {
    assertThat(udf.trunc(new BigDecimal("265.335"), 2), is(new BigDecimal("265.330")));
    assertThat(udf.trunc(new BigDecimal("-265.335"), 2), is(new BigDecimal("-265.330")));

    assertThat(udf.trunc(new BigDecimal("265.365"), 2), is(new BigDecimal("265.360")));
    assertThat(udf.trunc(new BigDecimal("-265.365"), 2), is(new BigDecimal("-265.360")));
  }

  @Test
  public void shouldHandleNullValues() {
    assertThat(udf.trunc((Integer) null), is((Long) null));
    assertThat(udf.trunc((Long) null), is((Long) null));
    assertThat(udf.trunc((Double) null), is((Long) null));
    assertThat(udf.trunc((Double) null), is((Long) null));
    assertThat(udf.trunc((BigDecimal) null), is((BigDecimal) null));
    assertThat(udf.trunc((Double) null, 2), is((Long) null));
    assertThat(udf.trunc((BigDecimal) null, 2), is((BigDecimal) null));
  }

  @Test
  public void shouldHandleNullDecimalPlaces() {
    assertThat(udf.trunc(1.75d, null), is(nullValue()));
    assertThat(udf.trunc(new BigDecimal("1.75"), null), is(nullValue()));
  }


  @Test
  public void shouldTruncateInt() {
    assertThat(udf.trunc(123), is(123L));
  }

  @Test
  public void shouldTruncateLong() {
    assertThat(udf.trunc(123L), is(123L));
  }

}