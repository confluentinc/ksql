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

package io.confluent.ksql.function.udf;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import org.junit.Before;
import org.junit.Test;

public class AsValueTest {

  private AsValue udf;

  @Before
  public void setUp()  {
    udf = new AsValue();
  }

  @Test
  public void shouldHandlePrimitiveTypes() {
    assertThat(udf.asValue(Boolean.TRUE), is(Boolean.TRUE));
    assertThat(udf.asValue(Integer.MIN_VALUE), is(Integer.MIN_VALUE));
    assertThat(udf.asValue(Long.MAX_VALUE), is(Long.MAX_VALUE));
    assertThat(udf.asValue(Double.MAX_VALUE), is(Double.MAX_VALUE));
    assertThat(udf.asValue("string"), is("string"));
  }

  @Test
  public void shouldHandleNullPrimitiveTypes() {
    assertThat(udf.asValue((Boolean)null), is(nullValue()));
    assertThat(udf.asValue((Integer) null), is(nullValue()));
    assertThat(udf.asValue((Long) null), is(nullValue()));
    assertThat(udf.asValue((Double) null), is(nullValue()));
    assertThat(udf.asValue((String) null), is(nullValue()));
  }
}