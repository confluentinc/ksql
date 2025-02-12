/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.function.udf.url;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import org.junit.Before;
import org.junit.Test;

public class UrlDecodeParamTest {

  private UrlDecodeParam decodeUdf;

  @Before
  public void setUp() {
    decodeUdf = new UrlDecodeParam();
  }

  @Test
  public void shouldDecodeEncodedValue() {
    assertThat(decodeUdf.decodeParam("%3Ffoo+%24bar"), equalTo("?foo $bar"));
  }

  @Test
  public void shouldReturnSpecialCharsIntact() {
    assertThat(".-*_ should all pass through without being encoded", decodeUdf.decodeParam("foo.-*_bar"), equalTo("foo.-*_bar"));
  }

  @Test
  public void shouldReturnEmptyStringForEmptyInput() {
    assertThat(decodeUdf.decodeParam(""), equalTo(""));
  }

  @Test
  public void shouldReturnForNullValue() {
    assertThat(decodeUdf.decodeParam(null), is(nullValue()));
  }
}
