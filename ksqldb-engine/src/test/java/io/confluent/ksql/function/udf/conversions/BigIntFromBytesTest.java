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

import org.junit.Test;
import java.nio.ByteBuffer;

public class BigIntFromBytesTest {
  private BigIntFromBytes udf = new BigIntFromBytes();

  @Test
  public void shouldConvertBytesToBigInteger() {
    assertThat(udf.bigIntFromBytes(toByteBuffer(5000000000L)), is(5000000000L));
  }

  @Test
  public void shouldConvertBytesToNegativeBigInteger() {
    assertThat(udf.bigIntFromBytes(toByteBuffer(-5000000000L)), is(-5000000000L));
  }

  @Test
  public void shouldConvertBytesToMaxBigInteger() {
    assertThat(udf.bigIntFromBytes(toByteBuffer(Long.MAX_VALUE)), is(Long.MAX_VALUE));
  }

  @Test
  public void shouldConvertBytesToMinBigInteger() {
    assertThat(udf.bigIntFromBytes(toByteBuffer(Long.MIN_VALUE)), is(Long.MIN_VALUE));
  }

  @Test
  public void shouldReturnNullOnNullBytes() {
    assertThat(udf.bigIntFromBytes(null), is(nullValue()));
  }

  @Test
  public void shouldReturnNullOnInvalidBytesSize() {
    assertThat(udf.bigIntFromBytes(
        ByteBuffer.wrap(new byte[]{1, 2, 3, 4})), is(nullValue()));
    assertThat(udf.bigIntFromBytes(
        ByteBuffer.wrap(new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9})), is(nullValue()));
  }

  private ByteBuffer toByteBuffer(final long n) {
    final ByteBuffer buffer = ByteBuffer.allocate(8);
    buffer.putLong(n);
    return buffer;
  }
}
