/*
 * Copyright 2021 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use this file except
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

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;

import io.confluent.ksql.function.KsqlFunctionException;
import io.confluent.ksql.util.KsqlException;
import org.junit.Before;
import org.junit.Test;

public class ConcatWSTest {

  private ConcatWS udf;

  @Before
  public void setUp() {
    udf = new ConcatWS();
  }

  @Test
  public void shouldConcatStrings() {
    assertThat(udf.concatWS(" ", "The", "Quick", "Brown", "Fox"), is("The Quick Brown Fox"));
  }

  @Test
  public void shouldConcatLongerSeparator() {
    final String result = udf.concatWS("SEP", "foo", "bar", "baz");
    assertThat(result, is("fooSEPbarSEPbaz"));
  }

  @Test
  public void shouldReturnSingleInputUnchanged() {
    assertThat(udf.concatWS("SEP", "singular"), is("singular"));
  }

  @Test
  public void shouldReturnNullForNullSeparator() {
    final Object result = udf.concatWS(null, "foo", "bar");
    assertThat(result, is(nullValue()));
  }

  @Test
  public void shouldReturnEmptyStringIfAllInputsNull() {
    final Object result = udf.concatWS("SEP", null, null);
    assertThat(result, is(""));
  }

  @Test
  public void shouldSkipAnyNullInputs() {
    final Object result = udf.concatWS("SEP", "foo", null, "bar");
    assertThat(result, is("fooSEPbar"));
  }

  @Test
  public void shouldFailIfOnlySeparatorInput() {
    // When:
    final KsqlException e = assertThrows(KsqlFunctionException.class, () -> udf.concatWS("SEP"));

    // Then:
    assertThat(e.getMessage(), containsString("expects at least two input arguments"));
  }

  @Test
  public void shouldWorkWithEmptySeparator() {
    final Object result = udf.concatWS("", "foo", "bar");
    assertThat(result, is("foobar"));
  }

  @Test
  public void shouldHandleEmptyInputs() {
    final Object result = udf.concatWS("SEP", "foo", "", "bar");
    assertThat(result, is("fooSEPSEPbar"));
  }

  @Test
  public void shouldReturnEmptyIfEverythingEmpty() {
    final Object result = udf.concatWS("", "", "");
    assertThat(result, is(""));
  }

}
