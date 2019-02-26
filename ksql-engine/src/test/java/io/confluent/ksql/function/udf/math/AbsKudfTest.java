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

import org.junit.Before;
import org.junit.Test;

public class AbsKudfTest {

  private AbsKudf udf;

  @Before
  public void setUp() {
    udf = new AbsKudf();
  }

  @Test
  public void shouldReturnNullWhenArgNull() {
    assertThat(udf.abs((Integer)null), is(nullValue()));
    assertThat(udf.abs((Long)null), is(nullValue()));
    assertThat(udf.abs((Double)null), is(nullValue()));
  }

  @Test
  public void shouldAcceptDoubleValues() {
    assertThat(udf.abs(-1.234), is(1.234));
    assertThat(udf.abs(5567.0), is(5567.0));
  }

  @Test
  public void shouldAcceptIntegerValues() {
    assertThat(udf.abs(-1), is(1));
    assertThat(udf.abs(1), is(1));
  }

  @Test
  public void shouldAcceptLongValues() {
    assertThat(udf.abs(-1L), is(1L));
    assertThat(udf.abs(1L), is(1L));
  }
}