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
    assertThat(udf.asin(-0.43), is(-0.444492776935819));
    assertThat(udf.asin(-0.5), is(-0.5235987755982989));
    assertThat(udf.asin(-1.0), is(-1.5707963267948966));
    assertThat(udf.asin(-1), is(-1.5707963267948966));
    assertThat(udf.asin(-1L), is(-1.5707963267948966));
  }

  @Test
  public void shouldHandleZero() {
    assertThat(udf.asin(0.0), is(0.0));
    assertThat(udf.asin(0), is(0.0));
    assertThat(udf.asin(0L), is(0.0));
  }

  @Test
  public void shouldHandlePositive() {
    assertThat(udf.asin(0.43), is(0.444492776935819));
    assertThat(udf.asin(0.5), is(0.5235987755982989));
    assertThat(udf.asin(1.0), is(1.5707963267948966));
    assertThat(udf.asin(1), is(1.5707963267948966));
    assertThat(udf.asin(1L), is(1.5707963267948966));
  }

  @Test
  public void shouldHandleMoreThanPositiveOne() {
    assertThat(Double.isNaN(udf.asin(1.1)), is(true));
    assertThat(Double.isNaN(udf.asin(6.0)), is(true));
    assertThat(Double.isNaN(udf.asin(2)), is(true));
    assertThat(Double.isNaN(udf.asin(2L)), is(true));
  }
}
