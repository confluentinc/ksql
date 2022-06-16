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
    assertThat(udf.sin(-9.1), is(-0.3190983623493521));
    assertThat(udf.sin(-6.3), is(-0.016813900484349713));
    assertThat(udf.sin(-7), is(-0.6569865987187891));
    assertThat(udf.sin(-7L), is(-0.6569865987187891));
  }

  @Test
  public void shouldHandleNegative() {
    assertThat(udf.sin(-0.43), is(-0.41687080242921076));
    assertThat(udf.sin(-Math.PI), closeTo(0, 0.000000000000001));
    assertThat(udf.sin(-2 * Math.PI), closeTo(0, 0.000000000000001));
    assertThat(udf.sin(-6), is(0.27941549819892586));
    assertThat(udf.sin(-6L), is(0.27941549819892586));
  }

  @Test
  public void shouldHandleZero() {
    assertThat(udf.sin(0.0), is(0.0));
    assertThat(udf.sin(0), is(0.0));
    assertThat(udf.sin(0L), is(0.0));
  }

  @Test
  public void shouldHandlePositive() {
    assertThat(udf.sin(0.43), is(0.41687080242921076));
    assertThat(udf.sin(Math.PI), closeTo(0, 0.000000000000001));
    assertThat(udf.sin(Math.PI * 2), closeTo(0, 0.000000000000001));
    assertThat(udf.sin(6), is(-0.27941549819892586));
    assertThat(udf.sin(6L), is(-0.27941549819892586));
  }

  @Test
  public void shouldHandleMoreThanPositive2Pi() {
    assertThat(udf.sin(9.1), is(0.3190983623493521));
    assertThat(udf.sin(6.3), is(0.016813900484349713));
    assertThat(udf.sin(7), is(0.6569865987187891));
    assertThat(udf.sin(7L), is(0.6569865987187891));
  }
}
