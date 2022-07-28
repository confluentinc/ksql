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
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class LogTest {
  private Log udf;

  @Before
  public void setUp() {
    udf = new Log();
  }

  @Test
  public void shouldHandleNull() {
    assertThat(udf.log((Integer)null), is(nullValue()));
    assertThat(udf.log((Long)null), is(nullValue()));
    assertThat(udf.log((Double)null), is(nullValue()));
    assertThat(udf.log(null, 13), is(nullValue()));
    assertThat(udf.log(null, 13L), is(nullValue()));
    assertThat(udf.log(null, 13.0), is(nullValue()));
    assertThat(udf.log(13, null), is(nullValue()));
    assertThat(udf.log(13L, null), is(nullValue()));
    assertThat(udf.log(13.0, null), is(nullValue()));
  }

  @Test
  public void shouldHandleNegativeValue() {
    assertThat(Double.isNaN(udf.log(-1)), is(true));
    assertThat(Double.isNaN(udf.log(-1L)), is(true));
    assertThat(Double.isNaN(udf.log(-1.0)), is(true));
    assertThat(Double.isNaN(udf.log(15, -1)), is(true));
    assertThat(Double.isNaN(udf.log(15L, -1L)), is(true));
    assertThat(Double.isNaN(udf.log(15.0, -1.0)), is(true));
  }

  @Test
  public void shouldHandleZeroValue() {
    assertThat(Double.isInfinite(udf.log(0)), is(true));
    assertThat(Double.isInfinite(udf.log(0L)), is(true));
    assertThat(Double.isInfinite(udf.log(0.0)), is(true));
    assertThat(Double.isInfinite(udf.log(15, 0)), is(true));
    assertThat(Double.isInfinite(udf.log(15L, 0L)), is(true));
    assertThat(Double.isInfinite(udf.log(15.0, 0.0)), is(true));
  }

  @Test
  public void shouldHandlePositiveValueAndBase() {
    assertThat(udf.log(1), closeTo(0.0, 0.000000000000001));
    assertThat(udf.log(1L), closeTo(0.0, 0.000000000000001));
    assertThat(udf.log(1.0), closeTo(0.0, 0.000000000000001));
    assertThat(udf.log(13), closeTo(2.5649493574615367, 0.000000000000001));
    assertThat(udf.log(13L), closeTo(2.5649493574615367, 0.000000000000001));
    assertThat(udf.log(13.0), closeTo(2.5649493574615367, 0.000000000000001));
    assertThat(udf.log(1), closeTo(0.0, 0.000000000000001));
    assertThat(udf.log(1L), closeTo(0.0, 0.000000000000001));
    assertThat(udf.log(1.0), closeTo(0.0, 0.000000000000001));
    assertThat(udf.log(15, 13), closeTo(0.9471572411831843, 0.000000000000001));
    assertThat(udf.log(15L, 13L), closeTo(0.9471572411831843, 0.000000000000001));
    assertThat(udf.log(15.0, 13.0), closeTo(0.9471572411831843, 0.000000000000001));
    assertThat(udf.log(Double.MIN_VALUE, 13.0), closeTo(-0.003445474597896734, 0.000000000000001));
    assertThat(udf.log(Double.MAX_VALUE, 13.0), closeTo(0.0036137106622471603, 0.000000000000001));
  }

  @Test
  public void shouldHandleNegativeBase() {
    assertThat(Double.isNaN(udf.log(-15, 13)), is(true));
    assertThat(Double.isNaN(udf.log(-15L, 13L)), is(true));
    assertThat(Double.isNaN(udf.log(-15.0, 13.0)), is(true));
  }

  @Test
  public void shouldHandleZeroBase() {
    assertThat(Double.isNaN(udf.log(0, 13)), is(true));
    assertThat(Double.isNaN(udf.log(0L, 13L)), is(true));
    assertThat(Double.isNaN(udf.log(0.0, 13.0)), is(true));
  }

  @Test
  public void shouldHandleOneBase() {
    assertThat(Double.isNaN(udf.log(1, 13)), is(true));
    assertThat(Double.isNaN(udf.log(1L, 13L)), is(true));
    assertThat(Double.isNaN(udf.log(1.0, 13.0)), is(true));
  }
}