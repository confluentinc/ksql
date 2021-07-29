/*
 * Copyright 2019 Confluent Inc.
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

import java.nio.ByteBuffer;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isEmptyOrNullString;
import static org.hamcrest.Matchers.nullValue;

public class ReplaceTest {
  private Replace udf;

  @Before
  public void setUp() {
    udf = new Replace();
  }

  @Test
  public void shouldHandleNull() {
    assertThat(udf.replace(null, "foo", "bar"), is(nullValue()));
    assertThat(udf.replace("foo", null, "bar"), is(nullValue()));
    assertThat(udf.replace("foo", "bar", null), is(nullValue()));
  }

  @Test
  public void shouldHandleNullBytes() {
    assertThat(udf.replace(null, BYTES_12, BYTES_45), is(nullValue()));
    assertThat(udf.replace(BYTES_123, null, BYTES_45), is(nullValue()));
    assertThat(udf.replace(BYTES_123, BYTES_12, null), is(nullValue()));
  }

  @Test
  public void shouldReplace() {
    assertThat(udf.replace("foobar", "foo", "bar"), is("barbar"));
    assertThat(udf.replace("foobar", "fooo", "bar"), is("foobar"));
    assertThat(udf.replace("foobar", "o", ""), is("fbar"));
    assertThat(udf.replace("abc", "", "n"), is("nanbncn"));
  }

  @Test
  public void shouldReplaceBytes() {
    assertThat(udf.replace(BYTES_123, BYTES_1, BYTES_45), is(ByteBuffer.wrap(new byte[]{4,5,2,3})));
    assertThat(udf.replace(BYTES_123, BYTES_45, BYTES_12), is(BYTES_123));
    assertThat(udf.replace(BYTES_123, BYTES_12, BYTES_45), is(ByteBuffer.wrap(new byte[]{4,5,3})));
    assertThat(udf.replace(BYTES_123, BYTES_12, EMPTY_BYTES), is(ByteBuffer.wrap(new byte[]{3})));
    assertThat(udf.replace(BYTES_45, EMPTY_BYTES, BYTES_1), is(ByteBuffer.wrap(new byte[]{1,4,1,5,1})));
  }


  final ByteBuffer BYTES_123 = ByteBuffer.wrap(new byte[]{1,2,3});
  final ByteBuffer BYTES_1 = ByteBuffer.wrap(new byte[]{1});
  final ByteBuffer BYTES_12 = ByteBuffer.wrap(new byte[]{1,2});
  final ByteBuffer BYTES_45 = ByteBuffer.wrap(new byte[]{4,5});
  final ByteBuffer EMPTY_BYTES = ByteBuffer.wrap(new byte[]{});
}