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
  public void shouldReturnNullOnInvalidBytesSize() {
    assertThat(udf.doubleFromBytes(
        ByteBuffer.wrap(new byte[]{1, 2, 3, 4})), is(nullValue()));
    assertThat(udf.doubleFromBytes(
        ByteBuffer.wrap(new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9})), is(nullValue()));
  }

  private ByteBuffer toByteBuffer(final double n) {
    final ByteBuffer buffer = ByteBuffer.allocate(8);
    buffer.putDouble(n);
    return buffer;
  }
}
