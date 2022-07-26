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

public class AsinTest {
  private Asin udf;

  @Before
  public void setUp() {
    udf = new Asin();
  }

  @Test
  public void shouldHandleNull() {
    assertThat(udf.asin((Integer) null), is(nullValue()));
    assertThat(udf.asin((Long) null), is(nullValue()));
    assertThat(udf.asin((Double) null), is(nullValue()));
  }

  @Test
  public void shouldHandleLessThanNegativeOne() {
    assertThat(Double.isNaN(udf.asin(-1.1)), is(true));
    assertThat(Double.isNaN(udf.asin(-6.0)), is(true));
    assertThat(Double.isNaN(udf.asin(-2)), is(true));
    assertThat(Double.isNaN(udf.asin(-2L)), is(true));
  }

  @Test
  public void shouldHandleNegative() {
    assertThat(udf.asin(-0.43), closeTo(-0.444492776935819, 0.000000000000001));
    assertThat(udf.asin(-0.5), closeTo(-0.5235987755982989, 0.000000000000001));
    assertThat(udf.asin(-1.0), closeTo(-1.5707963267948966, 0.000000000000001));
    assertThat(udf.asin(-1), closeTo(-1.5707963267948966, 0.000000000000001));
    assertThat(udf.asin(-1L), closeTo(-1.5707963267948966, 0.000000000000001));
  }

  @Test
  public void shouldHandleZero() {
    assertThat(udf.asin(0.0), closeTo(0.0, 0.000000000000001));
    assertThat(udf.asin(0), closeTo(0.0, 0.000000000000001));
    assertThat(udf.asin(0L), closeTo(0.0, 0.000000000000001));
  }

  @Test
  public void shouldHandlePositive() {
    assertThat(udf.asin(0.43), closeTo(0.444492776935819, 0.000000000000001));
    assertThat(udf.asin(0.5), closeTo(0.5235987755982989, 0.000000000000001));
    assertThat(udf.asin(1.0), closeTo(1.5707963267948966, 0.000000000000001));
    assertThat(udf.asin(1), closeTo(1.5707963267948966, 0.000000000000001));
    assertThat(udf.asin(1L), closeTo(1.5707963267948966, 0.000000000000001));
  }

  @Test
  public void shouldHandleMoreThanPositiveOne() {
    assertThat(Double.isNaN(udf.asin(1.1)), is(true));
    assertThat(Double.isNaN(udf.asin(6.0)), is(true));
    assertThat(Double.isNaN(udf.asin(2)), is(true));
    assertThat(Double.isNaN(udf.asin(2L)), is(true));
  }
}
