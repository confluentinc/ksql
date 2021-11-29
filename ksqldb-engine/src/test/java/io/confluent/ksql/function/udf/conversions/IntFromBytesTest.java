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

public class IntFromBytesTest {
  private IntFromBytes udf = new IntFromBytes();

  @Test
  public void shouldConvertBytesToInteger() {
    assertThat(udf.intFromBytes(toByteBuffer(2021)), is(2021));
  }

  @Test
  public void shouldConvertBytesToNegativeInteger() {
    assertThat(udf.intFromBytes(toByteBuffer(-2021)), is(-2021));
  }

  @Test
  public void shouldConvertBytesToMaxInteger() {
    assertThat(udf.intFromBytes(toByteBuffer(Integer.MAX_VALUE)), is(Integer.MAX_VALUE));
  }

  @Test
  public void shouldConvertBytesToMinInteger() {
    assertThat(udf.intFromBytes(toByteBuffer(Integer.MIN_VALUE)), is(Integer.MIN_VALUE));
  }

  @Test
  public void shouldReturnNullOnNullBytes() {
    assertThat(udf.intFromBytes(null), is(nullValue()));
  }

  @Test
  public void shouldThrowOnInvalidBytesSizes() {
    final Exception e1 = assertThrows(
        KsqlException.class,
        () -> udf.intFromBytes(ByteBuffer.wrap(new byte[]{1, 2})));

    assertThat(e1.getMessage(), is("Number of bytes must be equal to 4, but found 2"));

    final Exception e2 = assertThrows(
        KsqlException.class,
        () -> udf.intFromBytes(ByteBuffer.wrap(new byte[]{1, 2, 3, 4, 5})));

    assertThat(e2.getMessage(), is("Number of bytes must be equal to 4, but found 5"));
  }

  @Test
  public void shouldConvertLittleEndianBytesToInteger() {
    final ByteBuffer buffer = toByteBuffer(2021, ByteOrder.LITTLE_ENDIAN);
    assertThat(udf.intFromBytes(buffer, "LITTLE_ENDIAN"), is(2021));
  }

  @Test
  public void shouldConvertBigEndianBytesToInteger() {
    final ByteBuffer buffer = toByteBuffer(2021).order(ByteOrder.BIG_ENDIAN);
    assertThat(udf.intFromBytes(buffer, "BIG_ENDIAN"), is(2021));
  }

  @Test
  public void shouldThrowOnUnknownByteOrder() {
    final Exception e = assertThrows(
        KsqlException.class,
        () -> udf.intFromBytes(toByteBuffer(5), "weep!"));

    assertThat(e.getMessage(),
        is("Byte order must be BIG_ENDIAN or LITTLE_ENDIAN. Unknown byte order 'weep!'."));
  }

  private ByteBuffer toByteBuffer(final int n) {
    return toByteBuffer(n, ByteOrder.BIG_ENDIAN);
  }

  private ByteBuffer toByteBuffer(final int n, final ByteOrder byteOrder) {
    final ByteBuffer buffer = ByteBuffer.allocate(4);
    buffer.order(byteOrder);
    buffer.putInt(n);
    return buffer;
  }
}
