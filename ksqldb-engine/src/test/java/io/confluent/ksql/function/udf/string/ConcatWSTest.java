/*
 * Copyright 2020 Confluent Inc.
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
import java.nio.ByteBuffer;
import org.junit.Before;
import org.junit.Test;

public class ConcatWSTest {

  private ConcatWS udf;
  private static final ByteBuffer EMPTY_BYTES = ByteBuffer.wrap(new byte[] {});

  @Before
  public void setUp() {
    udf = new ConcatWS();
  }

  @Test
  public void shouldConcatStrings() {
    assertThat(udf.concatWS(" ", "The", "Quick", "Brown", "Fox"), is("The Quick Brown Fox"));
  }

  @Test
  public void shouldConcatBytes() {
    assertThat(udf.concatWS(ByteBuffer.wrap(new byte[] {1}), ByteBuffer.wrap(new byte[] {2}), ByteBuffer.wrap(new byte[] {3})),
        is(ByteBuffer.wrap(new byte[] {2, 1, 3})));
  }

  @Test
  public void shouldConcatLongerSeparator() {
    final String result = udf.concatWS("SEP", "foo", "bar", "baz");
    assertThat(result, is("fooSEPbarSEPbaz"));
  }

  @Test
  public void shouldReturnSingleInputUnchanged() {
    assertThat(udf.concatWS("SEP", "singular"), is("singular"));
    assertThat(udf.concatWS(ByteBuffer.wrap(new byte[] {1}), ByteBuffer.wrap(new byte[] {2})), is(ByteBuffer.wrap(new byte[] {2})));
  }

  @Test
  public void shouldReturnNullForNullSeparator() {
    assertThat(udf.concatWS(null, "foo", "bar"), is(nullValue()));
    assertThat(udf.concatWS(null, ByteBuffer.wrap(new byte[] {1}), ByteBuffer.wrap(new byte[] {2})), is(nullValue()));
  }

  @Test
  public void shouldReturnEmptyIfAllInputsNull() {
    assertThat(udf.concatWS("SEP", null, null), is(""));
    assertThat(udf.concatWS(ByteBuffer.wrap(new byte[] {2}), null, null), is(EMPTY_BYTES));
  }

  @Test
  public void shouldSkipAnyNullInputs() {
    assertThat(udf.concatWS("SEP", "foo", null, "bar"), is("fooSEPbar"));
    assertThat(udf.concatWS(ByteBuffer.wrap(new byte[] {1}), ByteBuffer.wrap(new byte[] {2}), null, ByteBuffer.wrap(new byte[] {3})),
        is(ByteBuffer.wrap(new byte[] {2, 1, 3})));
  }

  @Test
  public void shouldFailIfOnlySeparatorStringInput() {
    // When:
    final KsqlException e = assertThrows(KsqlFunctionException.class, () -> udf.concatWS("SEP"));

    // Then:
    assertThat(e.getMessage(), containsString("expects at least two input arguments"));
  }

  @Test
  public void shouldFailIfOnlySeparatorBytesInput() {
    // When:
    final KsqlException e = assertThrows(KsqlFunctionException.class, () -> udf.concatWS(ByteBuffer.wrap(new byte[] {3})));

    // Then:
    assertThat(e.getMessage(), containsString("expects at least two input arguments"));
  }

  @Test
  public void shouldWorkWithEmptySeparator() {
    assertThat(udf.concatWS("", "foo", "bar"), is("foobar"));
    assertThat(udf.concatWS(EMPTY_BYTES, ByteBuffer.wrap(new byte[] {1}), ByteBuffer.wrap(new byte[] {2})),
        is(ByteBuffer.wrap(new byte[] {1, 2})));
  }

  @Test
  public void shouldHandleEmptyInputs() {
    assertThat(udf.concatWS("SEP", "foo", "", "bar"), is("fooSEPSEPbar"));
    assertThat(udf.concatWS(ByteBuffer.wrap(new byte[] {1}), ByteBuffer.wrap(new byte[] {2}), EMPTY_BYTES, ByteBuffer.wrap(new byte[] {3})),
        is(ByteBuffer.wrap(new byte[] {2, 1, 1, 3})));
  }

  @Test
  public void shouldReturnEmptyIfEverythingEmpty() {
    assertThat(udf.concatWS("", "", ""), is(""));
    assertThat(udf.concatWS(EMPTY_BYTES, EMPTY_BYTES, EMPTY_BYTES), is(EMPTY_BYTES));
  }

}
