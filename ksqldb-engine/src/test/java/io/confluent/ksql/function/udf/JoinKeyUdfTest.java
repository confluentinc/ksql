/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.function.udf;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import org.junit.Before;
import org.junit.Test;

public class JoinKeyUdfTest {

  private JoinKeyUdf udf;

  @Before
  public void setUp()  {
    udf = new JoinKeyUdf();
  }

  @Test
  public void shouldHandlePrimitiveTypes() {
    assertThat(udf.joinKey(Boolean.TRUE, null), is(Boolean.TRUE));
    assertThat(udf.joinKey(Integer.MIN_VALUE, null), is(Integer.MIN_VALUE));
    assertThat(udf.joinKey(Long.MAX_VALUE, null), is(Long.MAX_VALUE));
    assertThat(udf.joinKey(Double.MAX_VALUE, null), is(Double.MAX_VALUE));
    assertThat(udf.joinKey("string", null), is("string"));
  }

  @Test
  public void shouldReturnFirstNonNull() {
    assertThat(udf.joinKey(null, 1), is(1));
    assertThat(udf.joinKey(2, 3), is(2));
    assertThat(udf.joinKey(4, null), is(4));
    assertThat(udf.joinKey(null, null), is(nullValue()));
  }
}