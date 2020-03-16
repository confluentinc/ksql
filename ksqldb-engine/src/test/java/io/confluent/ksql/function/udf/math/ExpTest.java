/*
 * Copyright 2018 Confluent Inc.
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

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

import org.junit.Before;
import org.junit.Test;

public class ExpTest {

  private Exp udf;

  @Before
  public void setUp() {
    udf = new Exp();
  }

  @Test
  public void shouldHandleNull() {
    assertThat(udf.exp((Integer) null), is(nullValue()));
    assertThat(udf.exp((Long)null), is(nullValue()));
    assertThat(udf.exp((Double)null), is(nullValue()));
  }

  @Test
  public void shouldHandleNegative() {
    assertThat(udf.exp(-1), is(0.36787944117144233));
    assertThat(udf.exp(-1L), is(0.36787944117144233));
    assertThat(udf.exp(-1.0), is(0.36787944117144233));
  }

  @Test
  public void shouldHandleZero() {
    assertThat(udf.exp(0), is(1.0));
    assertThat(udf.exp(0L), is(1.0));
    assertThat(udf.exp(0.0), is(1.0));
  }

  @Test
  public void shouldHandlePositive() {
    assertThat(udf.exp(1), is(2.718281828459045));
    assertThat(udf.exp(1L), is(2.718281828459045));
    assertThat(udf.exp(1.0), is(2.718281828459045));
  }
}