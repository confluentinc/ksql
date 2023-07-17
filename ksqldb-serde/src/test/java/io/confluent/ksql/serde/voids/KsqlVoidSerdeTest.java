/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.serde.voids;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.Before;
import org.junit.Test;

public class KsqlVoidSerdeTest {

  private Serializer<Void> serializer;
  private Deserializer<Void> deserializer;

  @Before
  public void setUp()  {
    serializer = new KsqlVoidSerde<Void>().serializer();
    deserializer = new KsqlVoidSerde<Void>().deserializer();
  }

  @Test
  public void shouldSerializeNullDataToNull() {
    // When:
    final byte[] result = serializer.serialize("t", null);

    // Then: did not throw and
    assertThat(result, is(nullValue()));
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  @Test
  public void shouldSerializeAnyDataToNull() {
    // When:
    final byte[] result = ((Serializer)serializer).serialize("t", "not null");

    // Then: did not throw and
    assertThat(result, is(nullValue()));
  }

  @Test
  public void shouldNotThrowOnDeserializationIfDataNotNull() {
    // When:
    final Void result = deserializer.deserialize("t", "not null".getBytes(UTF_8));

    // Then: did not throw and
    assertThat(result, is(nullValue()));
  }
}