/*
 * Copyright 2021 Confluent Inc.
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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import java.math.BigDecimal;
import org.junit.Before;
import org.junit.Test;

public class AbsTest {

  private Abs udf;

  @Before
  public void setUp() {
    udf = new Abs();
  }

  @Test
  public void shouldHandleNull() {
    assertThat(udf.abs((Integer) null), is(nullValue()));
    assertThat(udf.abs((Long)null), is(nullValue()));
    assertThat(udf.abs((Double)null), is(nullValue()));
    assertThat(udf.abs((BigDecimal) null), is(nullValue()));
  }

  @Test
  public void shouldHandleNegative() {
    assertThat(udf.abs(-1), is(1));
    assertThat(udf.abs(-1L), is(1L));
    assertThat(udf.abs(-1.0), is(1.0));
    assertThat(udf.abs(new BigDecimal(-1)), is(new BigDecimal(-1).abs()));
  }

  @Test
  public void shouldHandlePositive() {
    assertThat(udf.abs(1), is(1));
    assertThat(udf.abs(1L), is(1L));
    assertThat(udf.abs(1.0), is(1.0));
    assertThat(udf.abs(new BigDecimal(1)), is(new BigDecimal(1).abs()));
  }
}