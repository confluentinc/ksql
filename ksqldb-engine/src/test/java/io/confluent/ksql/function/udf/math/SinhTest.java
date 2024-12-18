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

public class SinhTest {
  private Sinh udf;

  @Before
  public void setUp() {
    udf = new Sinh();
  }

  @Test
  public void shouldHandleNull() {
    assertThat(udf.sinh((Integer) null), is(nullValue()));
    assertThat(udf.sinh((Long) null), is(nullValue()));
    assertThat(udf.sinh((Double) null), is(nullValue()));
  }

  @Test
  public void shouldHandleLessThanNegative2Pi() {
    assertThat(udf.sinh(-9.1), closeTo(-4477.64629590835, 0.000000000000001));
    assertThat(udf.sinh(-6.3), closeTo(-272.28503691057597, 0.000000000000001));
    assertThat(udf.sinh(-7), closeTo(-548.3161232732465, 0.000000000000001));
    assertThat(udf.sinh(-7L), closeTo(-548.3161232732465, 0.000000000000001));
  }

  @Test
  public void shouldHandleNegative() {
    assertThat(udf.sinh(-0.43), closeTo(-0.4433742144124824, 0.000000000000001));
    assertThat(udf.sinh(-Math.PI), closeTo(-11.548739357257748, 0.000000000000001));
    assertThat(udf.sinh(-Math.PI * 2), closeTo(-267.74489404101644, 0.000000000000001));
    assertThat(udf.sinh(-6), closeTo(-201.71315737027922, 0.000000000000001));
    assertThat(udf.sinh(-6L), closeTo(-201.71315737027922, 0.000000000000001));
  }

  @Test
  public void shouldHandleZero() {
    assertThat(udf.sinh(0.0), closeTo(0.0, 0.000000000000001));
    assertThat(udf.sinh(0), closeTo(0.0, 0.000000000000001));
    assertThat(udf.sinh(0L), closeTo(0.0, 0.000000000000001));
  }

  @Test
  public void shouldHandlePositive() {
    assertThat(udf.sinh(0.43), closeTo(0.4433742144124824, 0.000000000000001));
    assertThat(udf.sinh(Math.PI), closeTo(11.548739357257748, 0.000000000000001));
    assertThat(udf.sinh(Math.PI * 2), closeTo(267.74489404101644, 0.000000000000001));
    assertThat(udf.sinh(6), closeTo(201.71315737027922, 0.000000000000001));
    assertThat(udf.sinh(6L), closeTo(201.71315737027922, 0.000000000000001));
  }

  @Test
  public void shouldHandleMoreThanPositive2Pi() {
    assertThat(udf.sinh(9.1), closeTo(4477.64629590835, 0.000000000000001));
    assertThat(udf.sinh(6.3), closeTo(272.28503691057597, 0.000000000000001));
    assertThat(udf.sinh(7), closeTo(548.3161232732465, 0.000000000000001));
    assertThat(udf.sinh(7L), closeTo(548.3161232732465, 0.000000000000001));
  }
}
