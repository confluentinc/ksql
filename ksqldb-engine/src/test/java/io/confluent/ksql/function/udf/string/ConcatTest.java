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

package io.confluent.ksql.function.udf.string;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import org.junit.Before;
import org.junit.Test;

public class ConcatTest {

  private Concat udf;

  @Before
  public void setUp() {
    udf = new Concat();
  }

  @Test
  public void shouldConcatStrings() {
    assertThat(udf.concat("The", "Quick", "Brown", "Fox"), is("TheQuickBrownFox"));
  }

  @Test
  public void shouldIgnoreNullInputs() {
    assertThat(udf.concat(null, "this ", null, "should ", null, "work!", null),
        is("this should work!"));
  }

  @Test
  public void shouldReturnEmptyStringIfAllInputsNull() {
    assertThat(udf.concat(null, null), is(""));
  }

  @Test
  public void shouldReturnSingleInput() {
    assertThat(udf.concat("singular"), is("singular"));
  }

  @Test
  public void shouldReturnEmptyStringForSingleNullInput() {
    assertThat(udf.concat((String) null), is(""));
  }

}
