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

public class RadiansTest {
  private Radians udf;

  @Before
  public void setUp() {
    udf = new Radians();
  }

  @Test
  public void shouldHandleNull() {
    assertThat(udf.radians((Integer) null), is(nullValue()));
    assertThat(udf.radians((Long) null), is(nullValue()));
    assertThat(udf.radians((Double) null), is(nullValue()));
  }

  @Test
  public void shouldHandleNegative() {
    assertThat(udf.radians(-180.0), is(-Math.PI));
    assertThat(udf.radians(-360.0), is(-2 * Math.PI));
    assertThat(udf.radians(-70.73163980890013), is(-1.2345));
    assertThat(udf.radians(-114), is(-1.9896753472735358));
    assertThat(udf.radians(-114L), is(-1.9896753472735358));
  }

  @Test
  public void shouldHandleZero() {
    assertThat(udf.radians(0), is(0.0));
    assertThat(udf.radians(0L), is(0.0));
    assertThat(udf.radians(0.0), is(0.0));
  }

  @Test
  public void shouldHandlePositive() {
    assertThat(udf.radians(180.0), is(Math.PI));
    assertThat(udf.radians(360.0), is(2 * Math.PI));
    assertThat(udf.radians(70.73163980890013), is(1.2345));
    assertThat(udf.radians(114), is(1.9896753472735358));
    assertThat(udf.radians(114L), is(1.9896753472735358));
  }
}
