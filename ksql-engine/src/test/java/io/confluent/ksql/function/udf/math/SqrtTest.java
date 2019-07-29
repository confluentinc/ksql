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

public class SqrtTest {

  private Sqrt udf;

  @Before
  public void setUp() {
    udf = new Sqrt();
  }

  @Test
  public void shouldHandleNull() {
    assertThat(udf.sqrt((Integer)null), is(nullValue()));
    assertThat(udf.sqrt((Long)null), is(nullValue()));
    assertThat(udf.sqrt((Double)null), is(nullValue()));
  }

  @Test
  public void shouldHandleNegative() {
    assertThat(Double.isNaN(udf.sqrt(-6.0)), is(true));
  }

  @Test
  public void shouldHandleZero() {
    assertThat(udf.sqrt(0), is(0.0));
    assertThat(udf.sqrt(0L), is(0.0));
    assertThat(udf.sqrt(0.0), is(0.0));
  }

  @Test
  public void shouldHandlePositive() {
    assertThat(udf.sqrt(4), is(2.0));
    assertThat(udf.sqrt(3), is(1.7320508075688772));
    assertThat(udf.sqrt(1.0), is(1.0));
  }
}