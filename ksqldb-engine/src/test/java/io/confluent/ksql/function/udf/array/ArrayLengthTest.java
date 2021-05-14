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

package io.confluent.ksql.function.udf.array;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Test;

public class ArrayLengthTest {

  private ArrayLength udf;

  @Before
  public void setUp() {
    udf = new ArrayLength();
  }

  @Test
  public void shouldReturnNullForNullArray() {
    assertThat(udf.calcArrayLength(null), is(nullValue()));
  }

  @Test
  public void shouldReturnArraySize() {
    assertThat(udf.calcArrayLength(ImmutableList.of()), is(0));
    assertThat(udf.calcArrayLength(ImmutableList.of(1)), is(1));
    assertThat(udf.calcArrayLength(ImmutableList.of("one", "two")), is(2));
  }
}