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

package io.confluent.ksql.function.udf.string;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.nio.ByteBuffer;
import org.junit.Before;
import org.junit.Test;

public class ConcatTest {

  private Concat udf;

  @Before
  public void setUp() {
    udf = new Concat();
  }

  @Test
  public void shouldConcatStrings() {
    assertThat(udf.concat("The", "Quick", "Brown", "Fox"), is("TheQuickBrownFox"));
  }

  @Test
  public void shouldConcatBytes() {
    assertThat(udf.concat(ByteBuffer.wrap(new byte[] {1}), ByteBuffer.wrap(new byte[] {2}), ByteBuffer.wrap(new byte[] {3})),
        is(ByteBuffer.wrap(new byte[] {1, 2, 3})));
  }

  @Test
  public void shouldIgnoreNullInputs() {
    assertThat(udf.concat(null, "this ", null, "should ", null, "work!", null),
        is("this should work!"));
    assertThat(udf.concat(null, ByteBuffer.wrap(new byte[] {1}), null, ByteBuffer.wrap(new byte[] {2}), null),
        is(ByteBuffer.wrap(new byte[] {1, 2})));
  }

  @Test
  public void shouldReturnEmptyIfAllInputsNull() {
    assertThat(udf.concat((String) null, null), is(""));
    assertThat(udf.concat((ByteBuffer) null, null), is(ByteBuffer.wrap(new byte[] {})));
  }

  @Test
  public void shouldReturnSingleInput() {
    assertThat(udf.concat("singular"), is("singular"));
    assertThat(udf.concat(ByteBuffer.wrap(new byte[] {2})), is(ByteBuffer.wrap(new byte[] {2})));
  }

  @Test
  public void shouldReturnEmptyForSingleNullInput() {
    assertThat(udf.concat((String) null), is(""));
    assertThat(udf.concat((ByteBuffer) null), is(ByteBuffer.wrap(new byte[] {})));
  }

}
