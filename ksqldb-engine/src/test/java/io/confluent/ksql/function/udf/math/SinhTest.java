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
    assertThat(udf.sinh(-9.1), is(-4477.64629590835));
    assertThat(udf.sinh(-6.3), is(-272.28503691057597));
    assertThat(udf.sinh(-7), is(-548.3161232732465));
    assertThat(udf.sinh(-7L), is(-548.3161232732465));
  }

  @Test
  public void shouldHandleNegative() {
    assertThat(udf.sinh(-0.43), is(-0.4433742144124824));
    assertThat(udf.sinh(-Math.PI), is(-11.548739357257748));
    assertThat(udf.sinh(-Math.PI * 2), is(-267.74489404101644));
    assertThat(udf.sinh(-6), is(-201.71315737027922));
    assertThat(udf.sinh(-6L), is(-201.71315737027922));
  }

  @Test
  public void shouldHandleZero() {
    assertThat(udf.sinh(0.0), is(0.0));
    assertThat(udf.sinh(0), is(0.0));
    assertThat(udf.sinh(0L), is(0.0));
  }

  @Test
  public void shouldHandlePositive() {
    assertThat(udf.sinh(0.43), is(0.4433742144124824));
    assertThat(udf.sinh(Math.PI), is(11.548739357257748));
    assertThat(udf.sinh(Math.PI * 2), is(267.74489404101644));
    assertThat(udf.sinh(6), is(201.71315737027922));
    assertThat(udf.sinh(6L), is(201.71315737027922));
  }

  @Test
  public void shouldHandleMoreThanPositive2Pi() {
    assertThat(udf.sinh(9.1), is(4477.64629590835));
    assertThat(udf.sinh(6.3), is(272.28503691057597));
    assertThat(udf.sinh(7), is(548.3161232732465));
    assertThat(udf.sinh(7L), is(548.3161232732465));
  }
}
