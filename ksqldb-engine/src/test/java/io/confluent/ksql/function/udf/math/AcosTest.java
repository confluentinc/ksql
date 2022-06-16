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

public class AcosTest {
  private Acos udf;

  @Before
  public void setUp() {
    udf = new Acos();
  }

  @Test
  public void shouldHandleNull() {
    assertThat(udf.acos((Integer) null), is(nullValue()));
    assertThat(udf.acos((Long) null), is(nullValue()));
    assertThat(udf.acos((Double) null), is(nullValue()));
  }

  @Test
  public void shouldHandleLessThanNegativeOne() {
    assertThat(Double.isNaN(udf.acos(-1.1)), is(true));
    assertThat(Double.isNaN(udf.acos(-6.0)), is(true));
    assertThat(Double.isNaN(udf.acos(-2)), is(true));
    assertThat(Double.isNaN(udf.acos(-2L)), is(true));
  }

  @Test
  public void shouldHandleNegative() {
    assertThat(udf.acos(-0.43), is(2.0152891037307157));
    assertThat(udf.acos(-0.5), is(2.0943951023931957));
    assertThat(udf.acos(-1.0), is(3.141592653589793));
    assertThat(udf.acos(-1), is(3.141592653589793));
    assertThat(udf.acos(-1L), is(3.141592653589793));
  }

  @Test
  public void shouldHandleZero() {
    assertThat(udf.acos(0.0), is(1.5707963267948966));
    assertThat(udf.acos(0), is(1.5707963267948966));
    assertThat(udf.acos(0L), is(1.5707963267948966));
  }

  @Test
  public void shouldHandlePositive() {
    assertThat(udf.acos(0.43), is(1.1263035498590777));
    assertThat(udf.acos(0.5), is(1.0471975511965979));
    assertThat(udf.acos(1.0), is(0.0));
    assertThat(udf.acos(1), is(0.0));
    assertThat(udf.acos(1L), is(0.0));
  }

  @Test
  public void shouldHandleMoreThanPositiveOne() {
    assertThat(Double.isNaN(udf.acos(1.1)), is(true));
    assertThat(Double.isNaN(udf.acos(6.0)), is(true));
    assertThat(Double.isNaN(udf.acos(2)), is(true));
    assertThat(Double.isNaN(udf.acos(2L)), is(true));
  }
}
