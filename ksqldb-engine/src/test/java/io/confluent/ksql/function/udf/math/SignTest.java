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

import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class SignTest {
  private Sign udf;

  @Before
  public void setUp() {
    udf = new Sign();
  }

  @Test
  public void shouldHandleNull() {
    assertThat(udf.sign((Integer)null), is(nullValue()));
    assertThat(udf.sign((Long)null), is(nullValue()));
    assertThat(udf.sign((Double)null), is(nullValue()));
  }

  @Test
  public void shouldHandleNegative() {
    assertThat(udf.sign(-10.5), is(-1));
    assertThat(udf.sign(-10), is(-1));
    assertThat(udf.sign(-1L), is(-1));
  }

  @Test
  public void shouldHandleZero() {
    assertThat(udf.sign(0.0), is(0));
    assertThat(udf.sign(0), is(0));
  }

  @Test
  public void shouldHandlePositive() {
    assertThat(udf.sign(1), is(1));
    assertThat(udf.sign(1L), is(1));
    assertThat(udf.sign(1.5), is(1));
  }
}