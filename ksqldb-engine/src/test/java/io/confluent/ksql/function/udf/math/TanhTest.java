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

public class TanhTest {
  private Tanh udf;

  @Before
  public void setUp() {
    udf = new Tanh();
  }

  @Test
  public void shouldHandleNull() {
    assertThat(udf.tanh((Integer) null), is(nullValue()));
    assertThat(udf.tanh((Long) null), is(nullValue()));
    assertThat(udf.tanh((Double) null), is(nullValue()));
  }

  @Test
  public void shouldHandleLessThanNegative2Pi() {
    assertThat(udf.tanh(-9.1), closeTo(-0.9999999750614947, 0.000000000000001));
    assertThat(udf.tanh(-6.3), closeTo(-0.9999932559922726, 0.000000000000001));
    assertThat(udf.tanh(-7), closeTo(-0.9999983369439447, 0.000000000000001));
    assertThat(udf.tanh(-7L), closeTo(-0.9999983369439447, 0.000000000000001));
  }

  @Test
  public void shouldHandleNegative() {
    assertThat(udf.tanh(-0.43), closeTo(-0.4053213086894629, 0.000000000000001));
    assertThat(udf.tanh(-Math.PI), closeTo(-0.99627207622075, 0.000000000000001));
    assertThat(udf.tanh(-Math.PI * 2), closeTo(-0.9999930253396107, 0.000000000000001));
    assertThat(udf.tanh(-6), closeTo(-0.9999877116507956, 0.000000000000001));
    assertThat(udf.tanh(-6L), closeTo(-0.9999877116507956, 0.000000000000001));
  }

  @Test
  public void shouldHandleZero() {
    assertThat(udf.tanh(0.0), closeTo(0.0, 0.000000000000001));
    assertThat(udf.tanh(0), closeTo(0.0, 0.000000000000001));
    assertThat(udf.tanh(0L), closeTo(0.0, 0.000000000000001));
  }

  @Test
  public void shouldHandlePositive() {
    assertThat(udf.tanh(0.43), closeTo(0.4053213086894629, 0.000000000000001));
    assertThat(udf.tanh(Math.PI), closeTo(0.99627207622075, 0.000000000000001));
    assertThat(udf.tanh(Math.PI * 2), closeTo(0.9999930253396107, 0.000000000000001));
    assertThat(udf.tanh(6), closeTo(0.9999877116507956, 0.000000000000001));
    assertThat(udf.tanh(6L), closeTo(0.9999877116507956, 0.000000000000001));
  }

  @Test
  public void shouldHandleMoreThanPositive2Pi() {
    assertThat(udf.tanh(9.1), closeTo(0.9999999750614947, 0.000000000000001));
    assertThat(udf.tanh(6.3), closeTo(0.9999932559922726, 0.000000000000001));
    assertThat(udf.tanh(7), closeTo(0.9999983369439447, 0.000000000000001));
    assertThat(udf.tanh(7L), closeTo(0.9999983369439447, 0.000000000000001));
  }
}
