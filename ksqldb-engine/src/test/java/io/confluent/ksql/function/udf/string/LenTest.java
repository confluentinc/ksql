/*
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the
 * License.
 */

package io.confluent.ksql.function.udf.string;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

import org.junit.Before;
import org.junit.Test;

public class LenTest {

  private Len udf;

  @Before
  public void setUp() {
    udf = new Len();
  }

  @Test
  public void shouldComputeLength() {
    final Integer result = udf.len("FoO bAr");
    assertThat(result, is(7));
  }

  @Test
  public void shouldReturnZeroForEmptyInput() {
    final Integer result = udf.len("");
    assertThat(result, is(0));
  }

  @Test
  public void shouldReturnNullForNullInput() {
    final Integer result = udf.len(null);
    assertThat(result, is(nullValue()));
  }

}
