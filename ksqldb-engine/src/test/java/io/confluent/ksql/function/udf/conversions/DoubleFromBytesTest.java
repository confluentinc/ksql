/*
 * Copyright 2021 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.function.udf.conversions;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThrows;

import io.confluent.ksql.util.KsqlException;
import org.junit.Test;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class DoubleFromBytesTest {
  private DoubleFromBytes udf = new DoubleFromBytes();

  @Test
  public void shouldConvertBytesToDouble() {
    assertThat(udf.doubleFromBytes(toByteBuffer(532.8738323)), is(532.8738323));
  }

  @Test
  public void shouldConvertBytesToNegativeDouble() {
    assertThat(udf.doubleFromBytes(toByteBuffer(-532.8734)), is(-532.8734));
  }

  @Test
  public void shouldConvertBytesToMaxDouble() {
    assertThat(udf.doubleFromBytes(toByteBuffer(Double.MAX_VALUE)), is(Double.MAX_VALUE));
  }

  @Test
  public void shouldConvertBytesToMinDouble() {
    assertThat(udf.doubleFromBytes(toByteBuffer(Double.MIN_VALUE)), is(Double.MIN_VALUE));
  }

  @Test
  public void shouldReturnNullOnNullBytes() {
    assertThat(udf.doubleFromBytes(null), is(nullValue()));
  }

  @Test
  public void shouldThrowOnInvalidBytesSizes() {
    final Exception e1 = assertThrows(
        KsqlException.class,
        () -> udf.doubleFromBytes(ByteBuffer.wrap(new byte[]{1, 2, 3, 4})));

    assertThat(e1.getMessage(), is("Number of bytes must be equal to 8, but found 4"));

    final Exception e2 = assertThrows(
        KsqlException.class,
        () -> udf.doubleFromBytes(ByteBuffer.wrap(new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9})));

    assertThat(e2.getMessage(), is("Number of bytes must be equal to 8, but found 9"));
  }

  @Test
  public void shouldConvertLittleEndianBytesToInteger() {
    final ByteBuffer buffer = toByteBuffer(532.8738323, ByteOrder.LITTLE_ENDIAN);
    assertThat(udf.doubleFromBytes(buffer, "LITTLE_ENDIAN"), is(532.8738323));
  }

  @Test
  public void shouldConvertBigEndianBytesToInteger() {
    final ByteBuffer buffer = toByteBuffer(532.8738323).order(ByteOrder.BIG_ENDIAN);
    assertThat(udf.doubleFromBytes(buffer, "BIG_ENDIAN"), is(532.8738323));
  }

  @Test
  public void shouldThrowOnUnknownByteOrder() {
    final Exception e = assertThrows(
        KsqlException.class,
        () -> udf.doubleFromBytes(toByteBuffer(532.8738323), "weep!"));

    assertThat(e.getMessage(),
        is("Byte order must be BIG_ENDIAN or LITTLE_ENDIAN. Unknown byte order 'weep!'."));
  }

  private ByteBuffer toByteBuffer(final double n) {
    return toByteBuffer(n, ByteOrder.BIG_ENDIAN);
  }

  private ByteBuffer toByteBuffer(final double n, final ByteOrder byteOrder) {
    final ByteBuffer buffer = ByteBuffer.allocate(8);
    buffer.order(byteOrder);
    buffer.putDouble(n);
    return buffer;
  }
}
