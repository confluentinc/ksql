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

public class CotTest {
  private Cot udf;

  @Before
  public void setUp() {
    udf = new Cot();
  }

  @Test
  public void shouldHandleNull() {
    assertThat(udf.cot((Integer) null), is(nullValue()));
    assertThat(udf.cot((Long) null), is(nullValue()));
    assertThat(udf.cot((Double) null), is(nullValue()));
  }

  @Test
  public void shouldHandleLessThanNegative2Pi() {
    assertThat(udf.cot(-9.1), closeTo(2.9699983263892054, 0.000000000000001));
    assertThat(udf.cot(-6.3), closeTo(-59.46619211372627, 0.000000000000001));
    assertThat(udf.cot(-7), closeTo(-1.1475154224051356, 0.000000000000001));
    assertThat(udf.cot(-7L), closeTo(-1.1475154224051356, 0.000000000000001));
  }

  @Test
  public void shouldHandleNegative() {
    assertThat(udf.cot(-0.43), closeTo(-2.1804495406685085, 0.000000000000001));
    assertThat(udf.cot(-Math.PI), closeTo(8.165619676597685E15, 0.000000000000001));
    assertThat(udf.cot(-Math.PI * 2), closeTo(4.0828098382988425E15, 0.000000000000001));
    assertThat(udf.cot(-6), closeTo(3.436353004180128, 0.000000000000001));
    assertThat(udf.cot(-6L), closeTo(3.436353004180128, 0.000000000000001));
  }

  @Test
  public void shouldHandleZero() {
    assertThat(Double.isInfinite(udf.cot(0.0)), is(true));
    assertThat(Double.isInfinite(udf.cot(0)), is(true));
    assertThat(Double.isInfinite(udf.cot(0L)), is(true));
  }

  @Test
  public void shouldHandlePositive() {
    assertThat(udf.cot(0.43), closeTo(2.1804495406685085, 0.000000000000001));
    assertThat(udf.cot(Math.PI), closeTo(-8.165619676597685E15, 0.000000000000001));
    assertThat(udf.cot(Math.PI * 2), closeTo(-4.0828098382988425E15, 0.000000000000001));
    assertThat(udf.cot(6), closeTo(-3.436353004180128, 0.000000000000001));
    assertThat(udf.cot(6L), closeTo(-3.436353004180128, 0.000000000000001));
  }

  @Test
  public void shouldHandleMoreThanPositive2Pi() {
    assertThat(udf.cot(9.1), closeTo(-2.9699983263892054, 0.000000000000001));
    assertThat(udf.cot(6.3), closeTo(59.46619211372627, 0.000000000000001));
    assertThat(udf.cot(7), closeTo(1.1475154224051356, 0.000000000000001));
    assertThat(udf.cot(7L), closeTo(1.1475154224051356, 0.000000000000001));
  }
}
