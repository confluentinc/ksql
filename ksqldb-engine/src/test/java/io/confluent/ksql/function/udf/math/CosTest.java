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

public class CosTest {
  private Cos udf;

  @Before
  public void setUp() {
    udf = new Cos();
  }

  @Test
  public void shouldHandleNull() {
    assertThat(udf.cos((Integer) null), is(nullValue()));
    assertThat(udf.cos((Long) null), is(nullValue()));
    assertThat(udf.cos((Double) null), is(nullValue()));
  }

  @Test
  public void shouldHandleLessThanNegative2Pi() {
    assertThat(udf.cos(-9.1), is(-0.9477216021311119));
    assertThat(udf.cos(-6.3), is(0.9998586363834151));
    assertThat(udf.cos(-7), is(0.7539022543433046));
    assertThat(udf.cos(-7L), is(0.7539022543433046));
  }

  @Test
  public void shouldHandleNegative() {
    assertThat(udf.cos(-0.43), is(0.9089657496748851));
    assertThat(udf.cos(-Math.PI), is(-1.0));
    assertThat(udf.cos(-2 * Math.PI), is(1.0));
    assertThat(udf.cos(-6), is(0.960170286650366));
    assertThat(udf.cos(-6L), is(0.960170286650366));
  }

  @Test
  public void shouldHandleZero() {
    assertThat(udf.cos(0.0), is(1.0));
    assertThat(udf.cos(0), is(1.0));
    assertThat(udf.cos(0L), is(1.0));
  }

  @Test
  public void shouldHandlePositive() {
    assertThat(udf.cos(0.43), is(0.9089657496748851));
    assertThat(udf.cos(Math.PI), is(-1.0));
    assertThat(udf.cos(2 * Math.PI), is(1.0));
    assertThat(udf.cos(6), is(0.960170286650366));
    assertThat(udf.cos(6L), is(0.960170286650366));
  }

  @Test
  public void shouldHandleMoreThanPositive2Pi() {
    assertThat(udf.cos(9.1), is(-0.9477216021311119));
    assertThat(udf.cos(6.3), is(0.9998586363834151));
    assertThat(udf.cos(7), is(0.7539022543433046));
    assertThat(udf.cos(7L), is(0.7539022543433046));
  }
}
