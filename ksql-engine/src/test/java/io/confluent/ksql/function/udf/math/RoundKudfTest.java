/*
 * Copyright 2019 Confluent Inc.
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

import org.junit.Before;
import org.junit.Test;

public class RoundKudfTest {
  private RoundKudf udf;

  @Before
  public void setUp() {
    udf = new RoundKudf();
  }

  @Test
  public void shouldReturnNullWhenArgNull() {
    assertThat(udf.round((Integer)null), is(nullValue()));
    assertThat(udf.round((Long)null), is(nullValue()));
    assertThat(udf.round((Double)null), is(nullValue()));
  }

  @Test
  public void shouldAcceptDoubleValues() {
    assertThat(udf.round(-1.234), is(-1L));
    assertThat(udf.round(7.59), is(8L));
  }

  @Test
  public void shouldAcceptIntegerValues() {
    assertThat(udf.round(-1), is(-1L));
    assertThat(udf.round(1), is(1L));
  }

  @Test
  public void shouldAcceptLongValues() {
    assertThat(udf.round(-1L), is(-1L));
    assertThat(udf.round(1L), is(1L));
  }
}
