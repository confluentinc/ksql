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

public class Atan2Test {
  private Atan2 udf;

  @Before
  public void setUp() {
    udf = new Atan2();
  }

  @Test
  public void shouldHandleNull() {
    assertThat(udf.atan2(null, 1), is(nullValue()));
    assertThat(udf.atan2(null, 1L), is(nullValue()));
    assertThat(udf.atan2(null, 0.45), is(nullValue()));
    assertThat(udf.atan2(1, null), is(nullValue()));
    assertThat(udf.atan2(1L, null), is(nullValue()));
    assertThat(udf.atan2(0.45, null), is(nullValue()));
    assertThat(udf.atan2((Integer) null, null), is(nullValue()));
    assertThat(udf.atan2((Long) null, null), is(nullValue()));
    assertThat(udf.atan2((Double) null, null), is(nullValue()));
  }

  @Test
  public void shouldHandleNegativeYNegativeX() {
    assertThat(udf.atan2(-1.1, -0.24), closeTo(-1.7856117271965553, 0.000000000000001));
    assertThat(udf.atan2(-6.0, -7.1), closeTo(-2.4399674339361113, 0.000000000000001));
    assertThat(udf.atan2(-2, -3), closeTo(-2.5535900500422257, 0.000000000000001));
    assertThat(udf.atan2(-2L, -2L), closeTo(-2.356194490192345, 0.000000000000001));
  }

  @Test
  public void shouldHandleNegativeYPositiveX() {
    assertThat(udf.atan2(-1.1, 0.24), closeTo(-1.355980926393238, 0.000000000000001));
    assertThat(udf.atan2(-6.0, 7.1), closeTo(-0.7016252196536817, 0.000000000000001));
    assertThat(udf.atan2(-2, 3), closeTo(-0.5880026035475675, 0.000000000000001));
    assertThat(udf.atan2(-2L, 2L), closeTo(-0.7853981633974483, 0.000000000000001));
  }

  @Test
  public void shouldHandleNegativeYZeroX() {
    assertThat(udf.atan2(-1.1, 0.0), closeTo(-1.5707963267948966, 0.000000000000001));
    assertThat(udf.atan2(-6.0, 0.0), closeTo(-1.5707963267948966, 0.000000000000001));
    assertThat(udf.atan2(-2, 0), closeTo(-1.5707963267948966, 0.000000000000001));
    assertThat(udf.atan2(-2L, 0L), closeTo(-1.5707963267948966, 0.000000000000001));
  }

  @Test
  public void shouldHandleZeroYNegativeX() {
    assertThat(udf.atan2(0.0, -0.24), closeTo(3.141592653589793, 0.000000000000001));
    assertThat(udf.atan2(0.0, -7.1), closeTo(3.141592653589793, 0.000000000000001));
    assertThat(udf.atan2(0, -3), closeTo(3.141592653589793, 0.000000000000001));
    assertThat(udf.atan2(0L, -2L), closeTo(3.141592653589793, 0.000000000000001));
  }

  @Test
  public void shouldHandleZeroYPositiveX() {
    assertThat(udf.atan2(0.0, 0.24), closeTo(0.0, 0.000000000000001));
    assertThat(udf.atan2(0.0, 7.1), closeTo(0.0, 0.000000000000001));
    assertThat(udf.atan2(0, 3), closeTo(0.0, 0.000000000000001));
    assertThat(udf.atan2(0L, 2L), closeTo(0.0, 0.000000000000001));
  }

  @Test
  public void shouldHandleZeroYZeroX() {
    assertThat(udf.atan2(0.0, 0.0), closeTo(0.0, 0.000000000000001));
    assertThat(udf.atan2(0.0, 0.0), closeTo(0.0, 0.000000000000001));
    assertThat(udf.atan2(0, 0), closeTo(0.0, 0.000000000000001));
    assertThat(udf.atan2(0L, 0L), closeTo(0.0, 0.000000000000001));
  }

  @Test
  public void shouldHandlePositiveYNegativeX() {
    assertThat(udf.atan2(1.1, -0.24), closeTo(1.7856117271965553, 0.000000000000001));
    assertThat(udf.atan2(6.0, -7.1), closeTo(2.4399674339361113, 0.000000000000001));
    assertThat(udf.atan2(2, -3), closeTo(2.5535900500422257, 0.000000000000001));
    assertThat(udf.atan2(2L, -2L), closeTo(2.356194490192345, 0.000000000000001));
  }

  @Test
  public void shouldHandlePositiveYPositiveX() {
    assertThat(udf.atan2(1.1, 0.24), closeTo(1.355980926393238, 0.000000000000001));
    assertThat(udf.atan2(6.0, 7.1), closeTo(0.7016252196536817, 0.000000000000001));
    assertThat(udf.atan2(2, 3), closeTo(0.5880026035475675, 0.000000000000001));
    assertThat(udf.atan2(2L, 2L), closeTo(0.7853981633974483, 0.000000000000001));
  }

  @Test
  public void shouldHandlePositiveYZeroX() {
    assertThat(udf.atan2(1.1, 0.0), closeTo(1.5707963267948966, 0.000000000000001));
    assertThat(udf.atan2(6.0, 0.0), closeTo(1.5707963267948966, 0.000000000000001));
    assertThat(udf.atan2(2, 0), closeTo(1.5707963267948966, 0.000000000000001));
    assertThat(udf.atan2(2L, 0L), closeTo(1.5707963267948966, 0.000000000000001));
  }
}
