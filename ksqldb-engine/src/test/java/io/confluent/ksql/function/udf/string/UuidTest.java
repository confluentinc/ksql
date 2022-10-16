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

import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

public class UuidTest {

  private Uuid udf;

  @Before
  public void setUp() {
    udf = new Uuid();
  }

  @Test
  public void shouldReturnDistinctValueEachInvocation() {
    int capacity = 1000;
    final Set<String> outputs = new HashSet<String>(capacity);
    for (int i = 0; i < capacity; i++) {
      outputs.add(udf.uuid());
    }
    assertThat(outputs, hasSize(capacity));
  }

  @Test
  public void shouldHaveCorrectOutputFormat() {
    // aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee
    final String anUuid = udf.uuid();
    assertThat(anUuid.length(), is(36));
    assertThat(anUuid.charAt(8), is('-'));
    assertThat(anUuid.charAt(13), is('-'));
    assertThat(anUuid.charAt(18), is('-'));
    assertThat(anUuid.charAt(23), is('-'));
    for (final char c : anUuid.toCharArray()) {
      assertThat(c, isIn(Arrays.asList('a', 'b', 'c', 'd', 'e', 'f', '1', '2', '3', '4', '5', '6',
          '7', '8', '9', '0', '-')));
    }
  }

  @Test
  public void nullValueShouldReturnNullValue() {
    final String uuid = udf.uuid(null);

    assertThat(uuid, is(nullValue()));
  }

  @Test
  public void invalidCapacityShouldReturnNullValue() {
    final ByteBuffer bytes = ByteBuffer.wrap(new byte[17]);

    final String uuid = udf.uuid(bytes);

    assertThat(uuid, is(nullValue()));
  }

  @Test
  public void shouldReturnCorrectOutputFormat() {
    // aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee
    final String anUuid = udf.uuid();

    final java.util.UUID uuid = java.util.UUID.fromString(anUuid);
    final ByteBuffer bytes = ByteBuffer.wrap(new byte[16]);
    bytes.putLong(uuid.getMostSignificantBits());
    bytes.putLong(uuid.getLeastSignificantBits());
    byte[] byteArrays = bytes.array();

    final String toUuid = udf.uuid(ByteBuffer.wrap(byteArrays));
    assertThat(toUuid, is(anUuid));
  }
}