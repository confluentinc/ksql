/*
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
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

import java.math.BigDecimal;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

public class ModTest {

  private Mod udf;

  @Before
  public void setUp() {
    udf = new Mod();
  }

  @Test
  public void shouldModInt() {
    assertThat(udf.mod(10, null), is(nullValue()));
    assertThat(udf.mod(null, 3), is(nullValue()));
    assertThat(udf.mod((Integer) null, null), is(nullValue()));
    assertThat(udf.mod(10, 3), is(1));
    assertThat(udf.mod(10, 3), is(1));
    assertThat(udf.mod(10, -3), is(1));
    assertThat(udf.mod(-10, 3), is(-1));
    assertThat(udf.mod(-10, -3), is(-1));
  }

  @Test
  public void shouldModLong() {
    assertThat(udf.mod(10L, null), is(nullValue()));
    assertThat(udf.mod(null, 3L), is(nullValue()));
    assertThat(udf.mod((Long) null, null), is(nullValue()));
    assertThat(udf.mod(10L, 3L), is(1L));
    assertThat(udf.mod(10L, -3L), is(1L));
    assertThat(udf.mod(-10L, 3L), is(-1L));
    assertThat(udf.mod(-10L, -3L), is(-1L));
  }

  @Test
  public void shouldModDouble() {
    assertThat(udf.mod(10d, null), is(nullValue()));
    assertThat(udf.mod(null, 3d), is(nullValue()));
    assertThat(udf.mod((Double) null, null), is(nullValue()));
    assertThat(udf.mod(10d, 3d), is(1d));
    assertThat(udf.mod(10d, -3d), is(1d));
    assertThat(udf.mod(-10d, 3d), is(-1d));
    assertThat(udf.mod(-10d, -3d), is(-1d));
  }

  @Test
  public void shouldModBigDecimal() {
    assertThat(udf.mod(new BigDecimal(10), null), is(nullValue()));
    assertThat(udf.mod(null, new BigDecimal(3)), is(nullValue()));
    assertThat(udf.mod((BigDecimal) null, null), is(nullValue()));
    assertThat(udf.mod(new BigDecimal(10), new BigDecimal(3)), is(new BigDecimal(1)));
    assertThat(udf.mod(new BigDecimal(10), new BigDecimal(-3)), is(new BigDecimal(1)));
    assertThat(udf.mod(new BigDecimal(-10), new BigDecimal(3)), is(new BigDecimal(-1)));
    assertThat(udf.mod(new BigDecimal(-10), new BigDecimal(-3)), is(new BigDecimal(-1)));
  }
}