/*
 * Copyright 2018 Confluent Inc.
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

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

import io.confluent.ksql.function.udf.KudfTester;
import org.junit.Before;
import org.junit.Test;

public class RoundKudfTest {

  private RoundKudf udf;

  @Before
  public void setUp() {
    udf = new RoundKudf();
  }

  @Test
  public void shouldBeWellBehavedUdf() {
    new KudfTester(AbsKudf::new)
        .withArgumentTypes(Number.class)
        .test();
  }

  @Test
  public void shouldReturnNullWhenArgNull() {
    assertThat(udf.evaluate((Object)null), is(nullValue()));
  }

  @Test
  public void shouldRound() {
    assertThat(udf.evaluate(4.0), is(4L));
    assertThat(udf.evaluate(3.6456), is(4L));
    assertThat(udf.evaluate(3.6456, 2), is(3.65));
    assertThat(udf.evaluate(3.6456, 3), is(3.646));
  }

  @Test
  public void shouldRoundNegative() {
    assertThat(udf.evaluate(111.0, -1), is(110.0));
    assertThat(udf.evaluate(111.0, -2), is(100.0));
  }
}