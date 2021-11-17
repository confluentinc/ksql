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
  public void shouldReturnNullOnInvalidBytesSize() {
    assertThat(udf.intFromBytes(ByteBuffer.wrap(new byte[]{1, 2})), is(nullValue()));
    assertThat(udf.intFromBytes(ByteBuffer.wrap(new byte[]{1, 2, 3, 4, 5})), is(nullValue()));
  }

  private ByteBuffer toByteBuffer(final int n) {
    final ByteBuffer buffer = ByteBuffer.allocate(4);
    buffer.putInt(n);
    return buffer;
  }
}
