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

import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

public class CeilKudfTest {
  private CeilKudf udf;

  @Before
  public void setUp() {
    udf = new CeilKudf();
  }

  @Test
  public void shouldReturnNullWhenArgNull() {
    assertThat(udf.ceil((Integer)null), is(nullValue()));
    assertThat(udf.ceil((Long)null), is(nullValue()));
    assertThat(udf.ceil((Double)null), is(nullValue()));
  }

  @Test
  public void shouldAcceptDoubleValues() {
    assertThat(udf.ceil(-1.234), is(-1.0));
    assertThat(udf.ceil(7.59), is(8.0));
  }

  @Test
  public void shouldAcceptIntegerValues() {
    assertThat(udf.ceil(-1), is(-1));
    assertThat(udf.ceil(1), is(1));
  }

  @Test
  public void shouldAcceptLongValues() {
    assertThat(udf.ceil(-1L), is(-1L));
    assertThat(udf.ceil(1L), is(1L));
  }
}
