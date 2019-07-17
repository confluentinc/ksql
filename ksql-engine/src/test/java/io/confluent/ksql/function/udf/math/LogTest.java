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

import io.confluent.ksql.function.KsqlFunctionException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class LogTest {

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  private Log udf;

  @Before
  public void setUp() {
    udf = new Log();
  }

  @Test
  public void shouldHandleNull() {
    assertThat(udf.log((Integer)null), is(nullValue()));
    assertThat(udf.log((Long)null), is(nullValue()));
    assertThat(udf.log((Double)null), is(nullValue()));
  }

  @Test
  public void shouldThrowIfResultNan() {
    // Then:
    expectedException.expect(KsqlFunctionException.class);
    expectedException.expectMessage("Result was NaN");

    // When:
    udf.log(-1.0);
  }

  @Test
  public void shouldThrowIfInfinite() {
    // Then:
    expectedException.expect(KsqlFunctionException.class);
    expectedException.expectMessage("Result was infinite");

    // When:
    udf.log(0.0);
  }

  @Test
  public void shouldHandlePositive() {
    assertThat(udf.log(1), is(0.0));
    assertThat(udf.log(1L), is(0.0));
    assertThat(udf.log(1.0), is(0.0));
  }
}