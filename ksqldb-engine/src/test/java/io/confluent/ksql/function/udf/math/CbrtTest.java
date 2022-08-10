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

public class CbrtTest {

  private Cbrt udf;

  @Before
  public void setUp() {
    udf = new Cbrt();
  }

  @Test
  public void shouldHandleNull() {
    assertThat(udf.cbrt((Integer)null), is(nullValue()));
    assertThat(udf.cbrt((Long)null), is(nullValue()));
    assertThat(udf.cbrt((Double)null), is(nullValue()));
  }

  @Test
  public void shouldHandleNegative() {
    assertThat(udf.cbrt(-8), closeTo(-2.0, 0.000000000000001));
    assertThat(udf.cbrt(-3L), closeTo(-1.4422495703074083, 0.000000000000001));
    assertThat(udf.cbrt(-1.0), closeTo(-1.0, 0.000000000000001));
  }

  @Test
  public void shouldHandleZero() {
    assertThat(udf.cbrt(0), closeTo(0.0, 0.000000000000001));
    assertThat(udf.cbrt(0L), closeTo(0.0, 0.000000000000001));
    assertThat(udf.cbrt(0.0), closeTo(0.0, 0.000000000000001));
  }

  @Test
  public void shouldHandlePositive() {
    assertThat(udf.cbrt(8), closeTo(2.0, 0.000000000000001));
    assertThat(udf.cbrt(3L), closeTo(1.4422495703074083, 0.000000000000001));
    assertThat(udf.cbrt(1.0), closeTo(1.0, 0.000000000000001));
  }
}