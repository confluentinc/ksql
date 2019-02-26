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

public class FloorKudfTest {
  private FloorKudf udf;

  @Before
  public void setUp() {
    udf = new FloorKudf();
  }

  @Test
  public void shouldReturnNullWhenArgNull() {
    assertThat(udf.floor((Integer)null), is(nullValue()));
    assertThat(udf.floor((Long)null), is(nullValue()));
    assertThat(udf.floor((Double)null), is(nullValue()));
  }

  @Test
  public void shouldAcceptDoubleValues() {
    assertThat(udf.floor(-1.234), is(-2.0));
    assertThat(udf.floor(7.59), is(7.0));
  }

  @Test
  public void shouldAcceptIntegerValues() {
    assertThat(udf.floor(-1), is(-1));
    assertThat(udf.floor(1), is(1));
  }

  @Test
  public void shouldAcceptLongValues() {
    assertThat(udf.floor(-1L), is(-1L));
    assertThat(udf.floor(1L), is(1L));
  }
}