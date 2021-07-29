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

import io.confluent.ksql.util.BytesUtils;
import java.nio.ByteBuffer;
import org.junit.Test;

public class LPadTest {
  private final LPad udf = new LPad();
  private static final ByteBuffer BYTES_123 = ByteBuffer.wrap(new byte[]{1,2,3});
  private static final ByteBuffer BYTES_45 = ByteBuffer.wrap(new byte[]{4,5});
  private static final ByteBuffer EMPTY_BYTES = ByteBuffer.wrap(new byte[]{});

  @Test
  public void shouldPadInputString() {
    final String result = udf.lpad("foo", 7, "Bar");
    assertThat(result, is("BarBfoo"));
  }

  @Test
  public void shouldPadInputBytes() {
    final ByteBuffer result = udf.lpad(BYTES_123, 7, BYTES_45);
    assertThat(result, is(ByteBuffer.wrap(new byte[]{4,5,4,5,1,2,3})));
  }

  @Test
  public void shouldAppendPartialPaddingString() {
    final String result = udf.lpad("foo", 4, "Bar");
    assertThat(result, is("Bfoo"));
  }

  @Test
  public void shouldAppendPartialPaddingBytes() {
    final ByteBuffer result = udf.lpad(BYTES_123, 4, BYTES_45);
    assertThat(BytesUtils.getByteArray(result), is(new byte[]{4,1,2,3}));
  }

  @Test
  public void shouldReturnNullForNullInputString() {
    final String result = udf.lpad(null, 4, "foo");
    assertThat(result, is(nullValue()));
  }

  @Test
  public void shouldReturnNullForNullInputBytes() {
    final ByteBuffer result = udf.lpad(null, 4, BYTES_45);
    assertThat(result, is(nullValue()));
  }

  @Test
  public void shouldReturnNullForNullPaddingString() {
    final String result = udf.lpad("foo", 4, null);
    assertThat(result, is(nullValue()));
  }

  @Test
  public void shouldReturnNullForNullPaddingBytes() {
    final ByteBuffer result = udf.lpad(BYTES_123, 4, null);
    assertThat(result, is(nullValue()));
  }

  @Test
  public void shouldReturnNullForEmptyPaddingString() {
    final String result = udf.lpad("foo", 4, "");
    assertThat(result, is(nullValue()));
  }

  @Test
  public void shouldReturnNullForEmptyPaddingBytes() {
    final ByteBuffer result = udf.lpad(BYTES_123, 4, EMPTY_BYTES);
    assertThat(result, is(nullValue()));
  }

  @Test
  public void shouldPadEmptyInputString() {
    final String result = udf.lpad("", 4, "foo");
    assertThat(result, is("foof"));
  }

  @Test
  public void shouldPadEmptyInputBytes() {
    final ByteBuffer result = udf.lpad(EMPTY_BYTES, 4, BYTES_45);
    assertThat(result, is(ByteBuffer.wrap(new byte[]{4,5,4,5})));
  }

  @Test
  public void shouldTruncateInputIfTargetLengthTooSmallString() {
    final String result = udf.lpad("foo", 2, "bar");
    assertThat(result, is("fo"));
  }

  @Test
  public void shouldTruncateInputIfTargetLengthTooSmallBytes() {
    final ByteBuffer result = udf.lpad(BYTES_123, 2, BYTES_45);
    assertThat(result, is(ByteBuffer.wrap(new byte[]{1,2})));
  }

  @Test
  public void shouldReturnNullForNegativeLengthString() {
    final String result = udf.lpad("foo", -1, "bar");
    assertThat(result, is(nullValue()));
  }

  @Test
  public void shouldReturnNullForNegativeLengthBytes() {
    final ByteBuffer result = udf.lpad(BYTES_123, -1, BYTES_45);
    assertThat(result, is(nullValue()));
  }

  @Test
  public void shouldReturnNullForNullLengthString() {
    final String result = udf.lpad("foo", null, "bar");
    assertThat(result, is(nullValue()));
  }

  @Test
  public void shouldReturnNullForNullLengthBytes() {
    final ByteBuffer result = udf.lpad(BYTES_123, null, BYTES_45);
    assertThat(result, is(nullValue()));
  }

  @Test
  public void shouldReturnEmptyStringForZeroLength() {
    final String result = udf.lpad("foo", 0, "bar");
    assertThat(result, is(""));
  }

  @Test
  public void shouldReturnEmptyByteBufferForZeroLength() {
    final ByteBuffer result = udf.lpad(BYTES_123, 0, BYTES_45);
    assertThat(result, is(EMPTY_BYTES));
  }

}
