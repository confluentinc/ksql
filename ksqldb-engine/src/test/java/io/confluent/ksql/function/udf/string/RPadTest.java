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

public class RPadTest {
  private final RPad udf = new RPad();

  @Test
  public void shouldPadInputString() {
    final String result = udf.rpad("foo", 7, "Bar");
    assertThat(result, is("fooBarB"));
  }

  @Test
  public void shouldPadInputBytes() {
    final ByteBuffer result = udf.rpad(input(), 7, padding());
    assertThat(BytesUtils.getByteArray(result), is(new byte[]{1,2,3,4,5,4,5}));
  }

  @Test
  public void shouldAppendPartialPaddingString() {
    final String result = udf.rpad("foo", 4, "Bar");
    assertThat(result, is("fooB"));
  }

  @Test public void shouldAppendPartialPaddingBytes() {
    final ByteBuffer result = udf.rpad(input(), 4, padding());
    assertThat(BytesUtils.getByteArray(result), is(new byte[]{1,2,3,4}));
  }

  @Test
  public void shouldReturnNullForNullInputString() {
    final String result = udf.rpad(null, 4, "foo");
    assertThat(result, is(nullValue()));
  }

  @Test
  public void shouldReturnNullForNullInputBytes() {
    final ByteBuffer result = udf.rpad(null, 4, padding());
    assertThat(result, is(nullValue()));
  }

  @Test
  public void shouldReturnNullForNullPaddingString() {
    final String result = udf.rpad("foo", 4, null);
    assertThat(result, is(nullValue()));
  }

  @Test
  public void shouldReturnNullForNullPaddingBytes() {
    final ByteBuffer result = udf.rpad(input(), 4, null);
    assertThat(result, is(nullValue()));
  }

  @Test
  public void shouldReturnNullForEmptyPaddingString() {
    final String result = udf.rpad("foo", 4, "");
    assertThat(result, is(nullValue()));
  }

  @Test
  public void shouldReturnNullForEmptyPaddingBytes() {
    final ByteBuffer result = udf.rpad(input(), 4, empty());
    assertThat(result, is(nullValue()));
  }

  @Test
  public void shouldPadEmptyInputString() {
    final String result = udf.rpad("", 4, "foo");
    assertThat(result, is("foof"));
  }

  @Test
  public void shouldPadEmptyInputBytes() {
    final ByteBuffer result = udf.rpad(empty(), 4, padding());
    assertThat(result, is(ByteBuffer.wrap(new byte[]{4,5,4,5})));
  }

  @Test
  public void shouldTruncateInputIfTargetLengthTooSmallString() {
    final String result = udf.rpad("foo", 2, "bar");
    assertThat(result, is("fo"));
  }

  @Test
  public void shouldTruncateInputIfTargetLengthTooSmallBytes() {
    final ByteBuffer result = udf.rpad(input(), 2, padding());
    assertThat(result, is(ByteBuffer.wrap(new byte[]{1,2})));
  }

  @Test
  public void shouldReturnNullForNegativeLengthString() {
    final String result = udf.rpad("foo", -1, "bar");
    assertThat(result, is(nullValue()));
  }

  @Test
  public void shouldReturnNullForNegativeLengthBytes() {
    final ByteBuffer result = udf.rpad(input(), -1, padding());
    assertThat(result, is(nullValue()));
  }

  @Test
  public void shouldReturnEmptyStringForZeroLength() {
    final String result = udf.rpad("foo", 0, "bar");
    assertThat(result, is(""));
  }

  @Test
  public void shouldReturnEmptyByteBufferForZeroLength() {
    final ByteBuffer result = udf.rpad(input(), 0, padding());
    assertThat(result, is(empty()));
  }

  @Test
  public void shouldReturnNullForNullLengthString() {
    final String result = udf.rpad("foo", null, "bar");
    assertThat(result, is(nullValue()));
  }

  @Test
  public void shouldReturnNullForNullLengthBytes() {
    final ByteBuffer result = udf.rpad(input(), null, padding());
    assertThat(result, is(nullValue()));
  }

  private ByteBuffer input(){
    return ByteBuffer.wrap(new byte[]{1,2,3});
  }

  private ByteBuffer padding() {
    return ByteBuffer.wrap(new byte[]{4,5});
  }

  private ByteBuffer empty(){
    return ByteBuffer.wrap(new byte[]{});
  }

}
