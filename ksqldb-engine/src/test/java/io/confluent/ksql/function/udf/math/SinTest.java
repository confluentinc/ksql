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
import static org.hamcrest.number.IsCloseTo.closeTo;

public class SinTest {
  private Sin udf;

  @Before
  public void setUp() {
    udf = new Sin();
  }

  @Test
  public void shouldHandleNull() {
    assertThat(udf.sin((Integer) null), is(nullValue()));
    assertThat(udf.sin((Long) null), is(nullValue()));
    assertThat(udf.sin((Double) null), is(nullValue()));
  }

  @Test
  public void shouldHandleLessThanNegative2Pi() {
    assertThat(udf.sin(-9.1), closeTo(-0.3190983623493521, 0.000000000000001));
    assertThat(udf.sin(-6.3), closeTo(-0.016813900484349713, 0.000000000000001));
    assertThat(udf.sin(-7), closeTo(-0.6569865987187891, 0.000000000000001));
    assertThat(udf.sin(-7L), closeTo(-0.6569865987187891, 0.000000000000001));
  }

  @Test
  public void shouldHandleNegative() {
    assertThat(udf.sin(-0.43), closeTo(-0.41687080242921076, 0.000000000000001));
    assertThat(udf.sin(-Math.PI), closeTo(0, 0.000000000000001));
    assertThat(udf.sin(-2 * Math.PI), closeTo(0, 0.000000000000001));
    assertThat(udf.sin(-6), closeTo(0.27941549819892586, 0.000000000000001));
    assertThat(udf.sin(-6L), closeTo(0.27941549819892586, 0.000000000000001));
  }

  @Test
  public void shouldHandleZero() {
    assertThat(udf.sin(0.0), closeTo(0.0, 0.000000000000001));
    assertThat(udf.sin(0), closeTo(0.0, 0.000000000000001));
    assertThat(udf.sin(0L), closeTo(0.0, 0.000000000000001));
  }

  @Test
  public void shouldHandlePositive() {
    assertThat(udf.sin(0.43), closeTo(0.41687080242921076, 0.000000000000001));
    assertThat(udf.sin(Math.PI), closeTo(0, 0.000000000000001));
    assertThat(udf.sin(Math.PI * 2), closeTo(0, 0.000000000000001));
    assertThat(udf.sin(6), closeTo(-0.27941549819892586, 0.000000000000001));
    assertThat(udf.sin(6L), closeTo(-0.27941549819892586, 0.000000000000001));
  }

  @Test
  public void shouldHandleMoreThanPositive2Pi() {
    assertThat(udf.sin(9.1), closeTo(0.3190983623493521, 0.000000000000001));
    assertThat(udf.sin(6.3), closeTo(0.016813900484349713, 0.000000000000001));
    assertThat(udf.sin(7), closeTo(0.6569865987187891, 0.000000000000001));
    assertThat(udf.sin(7L), closeTo(0.6569865987187891, 0.000000000000001));
  }
}
