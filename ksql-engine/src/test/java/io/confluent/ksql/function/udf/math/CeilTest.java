/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
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

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

import java.math.BigDecimal;
import org.junit.Before;
import org.junit.Test;

public class CeilTest {

  private Ceil udf;

  @Before
  public void setUp() {
    udf = new Ceil();
  }

  @Test
  public void shouldHandleNull() {
    assertThat(udf.ceil((Double)null), is(nullValue()));
    assertThat(udf.ceil((BigDecimal) null), is(nullValue()));
  }

  @Test
  public void shouldHandleNegative() {
    assertThat(udf.ceil(-0.2), is(-0.0));
    assertThat(udf.ceil(-1.2), is(-1.0));
    assertThat(udf.ceil(-1.0), is(-1.0));
    assertThat(udf.ceil(new BigDecimal(-0.2)), is(new BigDecimal(-0)));
    assertThat(udf.ceil(new BigDecimal(-1.2)), is(new BigDecimal(-1)));
    assertThat(udf.ceil(new BigDecimal(-1.0)), is(new BigDecimal(-1)));
  }

  @Test
  public void shouldHandlePositive() {
    assertThat(udf.ceil(1.2), is(2.0));
    assertThat(udf.ceil(1.0), is(1.0));
    assertThat(udf.ceil(new BigDecimal(1.2)), is(new BigDecimal(2)));
    assertThat(udf.ceil(new BigDecimal(1.0)), is(new BigDecimal(1)));
  }
}
