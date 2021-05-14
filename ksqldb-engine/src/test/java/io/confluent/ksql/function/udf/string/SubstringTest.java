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
import static org.hamcrest.Matchers.nullValue;

import org.junit.Before;
import org.junit.Test;

public class SubstringTest {

  private Substring udf;

  @Before
  public void setUp() {
    udf = new Substring();
  }

  @Test
  public void shouldReturnNullOnNullValue() {
    assertThat(udf.substring(null, 1), is(nullValue()));
    assertThat(udf.substring(null, 1, 1), is(nullValue()));
    assertThat(udf.substring("some string", null, 1), is(nullValue()));
    assertThat(udf.substring("some string", 1, null), is(nullValue()));
  }

  @Test
  public void shouldUseOneBasedIndexing() {
    assertThat(udf.substring("a test string", 1, 1), is("a"));
    assertThat(udf.substring("a test string", -1, 1), is("g"));
  }

  @Test
  public void shouldExtractFromStartForPositivePositions() {
    assertThat(udf.substring("a test string", 3), is("test string"));
    assertThat(udf.substring("a test string", 3, 4), is("test"));
  }

  @Test
  public void shouldExtractFromEndForNegativePositions() {
    assertThat(udf.substring("a test string", -6), is("string"));
    assertThat(udf.substring("a test string", -6, 2), is("st"));
  }

  @Test
  public void shouldTruncateOutOfBoundIndexes() {
    assertThat(udf.substring("a test string", 0), is("a test string"));
    assertThat(udf.substring("a test string", 100), is(""));
    assertThat(udf.substring("a test string", -100), is("a test string"));
    assertThat(udf.substring("a test string", 3, 100), is("test string"));
    assertThat(udf.substring("a test string", 3, -100), is(""));
  }

}