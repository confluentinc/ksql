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

package io.confluent.ksql.test.serde.json;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.google.common.collect.ImmutableMap;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import java.math.BigDecimal;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ValueSpecJsonSerdeSupplierTest {

  @Mock
  private SchemaRegistryClient srClient;

  private ValueSpecJsonSerdeSupplier plainSerde;

  @Before
  public void setUp() {
    plainSerde = new ValueSpecJsonSerdeSupplier(ImmutableMap.of());
  }

  @Test
  public void shouldSerializeDecimalsWithOutStrippingTrailingZeros_Plain() {
    // Given:
    final Serializer<Object> serializer = plainSerde.getSerializer(srClient, false);

    // When:
    final byte[] bytes = serializer.serialize("t", new BigDecimal("10.0"));

    // Then:
    assertThat(new String(bytes, UTF_8), is("10.0"));
  }

  @Test
  public void shouldDeserializeDecimalsWithoutStrippingTrailingZeros_Plain() {
    // Given:
    final Deserializer<Object> deserializer = plainSerde.getDeserializer(srClient, false);

    final byte[] bytes = "10.0".getBytes(UTF_8);

    // When:
    final Object result = deserializer.deserialize("t", bytes);

    // Then:
    assertThat(result, is(new BigDecimal("10.0")));
  }
}