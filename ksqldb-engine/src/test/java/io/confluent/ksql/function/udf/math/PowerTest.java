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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class PowerTest {
  private Power udf;

  @Before
  public void setUp() {
    udf = new Power();
  }

  @Test
  public void shouldHandleNull() {
    assertThat(udf.power(null, 13), is(nullValue()));
    assertThat(udf.power(null, 13L), is(nullValue()));
    assertThat(udf.power(null, 13.0), is(nullValue()));
    assertThat(udf.power(13, null), is(nullValue()));
    assertThat(udf.power(13L, null), is(nullValue()));
    assertThat(udf.power(13.0, null), is(nullValue()));
  }

  @Test
  public void shouldHandleNegativeExponent() {
    assertThat(udf.power(15, -1), is(0.06666666666666667));
    assertThat(udf.power(15L, -1L), is(0.06666666666666667));
    assertThat(udf.power(15.0, -1.0), is(0.06666666666666667));
    assertThat(udf.power(15, -2), is(0.0044444444444444444));
    assertThat(udf.power(15L, -2L), is(0.0044444444444444444));
    assertThat(udf.power(15.0, -2.0), is(0.0044444444444444444));
  }

  @Test
  public void shouldHandleZeroExponent() {
    assertThat(udf.power(15, 0), is(1.0));
    assertThat(udf.power(15L, 0L), is(1.0));
    assertThat(udf.power(15.0, 0.0), is(1.0));
    assertThat(udf.power(0, 0), is(1.0));
    assertThat(udf.power(0L, 0L), is(1.0));
    assertThat(udf.power(0.0, 0.0), is(1.0));
  }

  @Test
  public void shouldHandlePositiveExponent() {
    assertThat(udf.power(1, 5), is(1.0));
    assertThat(udf.power(1L, 5L), is(1.0));
    assertThat(udf.power(1.0, 5.0), is(1.0));
    assertThat(udf.power(15, 13), is(1.946195068359375E15));
    assertThat(udf.power(15L, 13L), is(1.946195068359375E15));
    assertThat(udf.power(15.0, 13.0), is(1.946195068359375E15));
    assertThat(udf.power(Double.MIN_VALUE, 13.0), is(0.0));
    assertThat(udf.power(Double.MAX_VALUE, 13.0), is(Double.POSITIVE_INFINITY));
  }

  @Test
  public void shouldHandleNegativeBase() {
    assertThat(udf.power(-15, 2), is(225.0));
    assertThat(udf.power(-15L, 2L), is(225.0));
    assertThat(udf.power(-15.0, 2.0), is(225.0));
    assertThat(udf.power(-15, 3), is(-3375.0));
    assertThat(udf.power(-15L, 3L), is(-3375.0));
    assertThat(udf.power(-15.0, 3.0), is(-3375.0));
  }

  @Test
  public void shouldHandleZeroBase() {
    assertThat(udf.power(0, 13), is(0.0));
    assertThat(udf.power(0L, 13L), is(0.0));
    assertThat(udf.power(0.0, 13.0), is(0.0));
  }

  @Test
  public void shouldHandleOneBase() {
    assertThat(udf.power(1, 13), is(1.0));
    assertThat(udf.power(1L, 13L), is(1.0));
    assertThat(udf.power(1.0, 13.0), is(1.0));
  }
}