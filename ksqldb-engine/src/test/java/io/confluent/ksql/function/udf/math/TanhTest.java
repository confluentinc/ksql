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
    assertThat(udf.tanh(-9.1), is(-0.9999999750614947));
    assertThat(udf.tanh(-6.3), is(-0.9999932559922726));
    assertThat(udf.tanh(-7), is(-0.9999983369439447));
    assertThat(udf.tanh(-7L), is(-0.9999983369439447));
  }

  @Test
  public void shouldHandleNegative() {
    assertThat(udf.tanh(-0.43), is(-0.4053213086894629));
    assertThat(udf.tanh(-Math.PI), is(-0.99627207622075));
    assertThat(udf.tanh(-Math.PI * 2), is(-0.9999930253396107));
    assertThat(udf.tanh(-6), is(-0.9999877116507956));
    assertThat(udf.tanh(-6L), is(-0.9999877116507956));
  }

  @Test
  public void shouldHandleZero() {
    assertThat(udf.tanh(0.0), is(0.0));
    assertThat(udf.tanh(0), is(0.0));
    assertThat(udf.tanh(0L), is(0.0));
  }

  @Test
  public void shouldHandlePositive() {
    assertThat(udf.tanh(0.43), is(0.4053213086894629));
    assertThat(udf.tanh(Math.PI), is(0.99627207622075));
    assertThat(udf.tanh(Math.PI * 2), is(0.9999930253396107));
    assertThat(udf.tanh(6), is(0.9999877116507956));
    assertThat(udf.tanh(6L), is(0.9999877116507956));
  }

  @Test
  public void shouldHandleMoreThanPositive2Pi() {
    assertThat(udf.tanh(9.1), is(0.9999999750614947));
    assertThat(udf.tanh(6.3), is(0.9999932559922726));
    assertThat(udf.tanh(7), is(0.9999983369439447));
    assertThat(udf.tanh(7L), is(0.9999983369439447));
  }
}
