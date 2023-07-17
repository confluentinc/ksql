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

public class AtanTest {
  private Atan udf;

  @Before
  public void setUp() {
    udf = new Atan();
  }

  @Test
  public void shouldHandleNull() {
    assertThat(udf.atan((Integer) null), is(nullValue()));
    assertThat(udf.atan((Long) null), is(nullValue()));
    assertThat(udf.atan((Double) null), is(nullValue()));
  }

  @Test
  public void shouldHandleLessThanNegativeOne() {
    assertThat(udf.atan(-1.1), closeTo(-0.8329812666744317, 0.000000000000001));
    assertThat(udf.atan(-6.0), closeTo(-1.4056476493802699, 0.000000000000001));
    assertThat(udf.atan(-2), closeTo(-1.1071487177940904, 0.000000000000001));
    assertThat(udf.atan(-2L), closeTo(-1.1071487177940904, 0.000000000000001));
  }

  @Test
  public void shouldHandleNegative() {
    assertThat(udf.atan(-0.43), closeTo(-0.40609805831761564, 0.000000000000001));
    assertThat(udf.atan(-0.5), closeTo(-0.4636476090008061, 0.000000000000001));
    assertThat(udf.atan(-1.0), closeTo(-0.7853981633974483, 0.000000000000001));
    assertThat(udf.atan(-1), closeTo(-0.7853981633974483, 0.000000000000001));
    assertThat(udf.atan(-1L), closeTo(-0.7853981633974483, 0.000000000000001));
  }

  @Test
  public void shouldHandleZero() {
    assertThat(udf.atan(0.0), closeTo(0.0, 0.000000000000001));
    assertThat(udf.atan(0), closeTo(0.0, 0.000000000000001));
    assertThat(udf.atan(0L), closeTo(0.0, 0.000000000000001));
  }

  @Test
  public void shouldHandlePositive() {
    assertThat(udf.atan(0.43), closeTo(0.40609805831761564, 0.000000000000001));
    assertThat(udf.atan(0.5), closeTo(0.4636476090008061, 0.000000000000001));
    assertThat(udf.atan(1.0), closeTo(0.7853981633974483, 0.000000000000001));
    assertThat(udf.atan(1), closeTo(0.7853981633974483, 0.000000000000001));
    assertThat(udf.atan(1L), closeTo(0.7853981633974483, 0.000000000000001));
  }

  @Test
  public void shouldHandleMoreThanPositiveOne() {
    assertThat(udf.atan(1.1), closeTo(0.8329812666744317, 0.000000000000001));
    assertThat(udf.atan(6.0), closeTo(1.4056476493802699, 0.000000000000001));
    assertThat(udf.atan(2), closeTo(1.1071487177940904, 0.000000000000001));
    assertThat(udf.atan(2L), closeTo(1.1071487177940904, 0.000000000000001));
  }
}
