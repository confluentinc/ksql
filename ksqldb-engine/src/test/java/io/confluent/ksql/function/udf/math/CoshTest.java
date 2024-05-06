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

public class CoshTest {
  private Cosh udf;

  @Before
  public void setUp() {
    udf = new Cosh();
  }

  @Test
  public void shouldHandleNull() {
    assertThat(udf.cosh((Integer) null), is(nullValue()));
    assertThat(udf.cosh((Long) null), is(nullValue()));
    assertThat(udf.cosh((Double) null), is(nullValue()));
  }

  @Test
  public void shouldHandleLessThanNegative2Pi() {
    assertThat(udf.cosh(-9.1), closeTo(4477.646407574158, 0.000000000000001));
    assertThat(udf.cosh(-6.3), closeTo(272.286873215353, 0.000000000000001));
    assertThat(udf.cosh(-7), closeTo(548.317035155212, 0.000000000000001));
    assertThat(udf.cosh(-7L), closeTo(548.317035155212, 0.000000000000001));
  }

  @Test
  public void shouldHandleNegative() {
    assertThat(udf.cosh(-0.43), closeTo(1.0938833091357991, 0.000000000000001));
    assertThat(udf.cosh(-Math.PI), closeTo(11.591953275521519, 0.000000000000001));
    assertThat(udf.cosh(-Math.PI * 2), closeTo(267.7467614837482, 0.000000000000001));
    assertThat(udf.cosh(-6), closeTo(201.7156361224559, 0.000000000000001));
    assertThat(udf.cosh(-6L), closeTo(201.7156361224559, 0.000000000000001));
  }

  @Test
  public void shouldHandleZero() {
    assertThat(udf.cosh(0.0), closeTo(1.0, 0.000000000000001));
    assertThat(udf.cosh(0), closeTo(1.0, 0.000000000000001));
    assertThat(udf.cosh(0L), closeTo(1.0, 0.000000000000001));
  }

  @Test
  public void shouldHandlePositive() {
    assertThat(udf.cosh(0.43), closeTo(1.0938833091357991, 0.000000000000001));
    assertThat(udf.cosh(Math.PI), closeTo(11.591953275521519, 0.000000000000001));
    assertThat(udf.cosh(Math.PI * 2), closeTo(267.7467614837482, 0.000000000000001));
    assertThat(udf.cosh(6), closeTo(201.7156361224559, 0.000000000000001));
    assertThat(udf.cosh(6L), closeTo(201.7156361224559, 0.000000000000001));
  }

  @Test
  public void shouldHandleMoreThanPositive2Pi() {
    assertThat(udf.cosh(9.1), closeTo(4477.646407574158, 0.000000000000001));
    assertThat(udf.cosh(6.3), closeTo(272.286873215353, 0.000000000000001));
    assertThat(udf.cosh(7), closeTo(548.317035155212, 0.000000000000001));
    assertThat(udf.cosh(7L), closeTo(548.317035155212, 0.000000000000001));
  }
}
