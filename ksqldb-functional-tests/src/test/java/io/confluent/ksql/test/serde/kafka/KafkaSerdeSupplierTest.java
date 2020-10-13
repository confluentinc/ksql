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

package io.confluent.ksql.test.serde.kafka;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import java.math.BigDecimal;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.Test;

public class KafkaSerdeSupplierTest {

  @Test
  public void shouldSerializeKeyAsStringIfNoKeyColumnsToAllowTestsToProveKeyIsIgnored() {
    // Given:
    final Serializer<Object> serializer = getSerializer(SqlTypes.BIGINT);

    serializer.configure(ImmutableMap.of(), true);

    // When:
    final byte[] data = serializer.serialize("t", "22");

    // Then:
    assertThat(new String(data, UTF_8), is("22"));
  }

  @Test
  public void shouldDeserializeKeyAsStringIfNoKeyColumnsToAllowTestsToProveKeyIsIgnored() {
    // Given:
    final Deserializer<?> deserializer = getDeserializer(SqlTypes.BIGINT);

    deserializer.configure(ImmutableMap.of(), true);

    // When:
    final Object result = deserializer.deserialize("t", "22".getBytes(UTF_8));

    // Then:
    assertThat(result, is("22"));
  }

  @Test
  public void shouldSerializeIntAsLong() {
    // Given:
    final Serializer<Object> serializer = getSerializer(SqlTypes.BIGINT);

    // When:
    final byte[] result = serializer.serialize(null, 10);

    assertThat(result, is(new byte[]{0, 0, 0, 0, 0, 0, 0, 10}));
  }

  @Test
  public void shouldDeserializeSmallLongAsInt() {
    // Given:
    final Deserializer<?> deserializer = getDeserializer(SqlTypes.BIGINT);

    // When:
    final Object result = deserializer.deserialize(null, new byte[]{0, 0, 0, 0, 0, 0, 0, 11});

    assertThat(result, is(11));
  }

  @Test
  public void shouldDeserializeBigLongAsLong() {
    // Given:
    final Deserializer<?> deserializer = getDeserializer(SqlTypes.BIGINT);

    // When:
    final Object result = deserializer.deserialize(null, new byte[]{0, 0, 1, 0, 0, 0, 0, 11});

    assertThat(result, is(1099511627787L));
  }

  @Test
  public void shouldSerializeIntAsDouble() {
    // Given:
    final Serializer<Object> serializer = getSerializer(SqlTypes.DOUBLE);

    // When:
    final byte[] result = serializer.serialize(null, 234);

    assertThat(result, is(new byte[]{64, 109, 64, 0, 0, 0, 0, 0}));
  }

  @Test
  public void shouldSerializeLongAsDouble() {
    // Given:
    final Serializer<Object> serializer = getSerializer(SqlTypes.DOUBLE);

    // When:
    final byte[] result = serializer.serialize(null, 1099511627787L);

    assertThat(result, is(new byte[]{66, 112, 0, 0, 0, 0, -80, 0}));
  }

  @Test
  public void shouldDeserializeDoubleAsDecimal() {
    // Given:
    final Deserializer<?> deserializer = getDeserializer(SqlTypes.DOUBLE);

    // When:
    final Object result = deserializer.deserialize(null, new byte[]{66, 112, 0, 0, 0, 0, -80, 0});

    assertThat(result, is(new BigDecimal("1099511627787")));
  }

  private static Serializer<Object> getSerializer(final SqlType colType) {
    final Serializer<Object> serializer = getSerdeSupplier(colType).getSerializer(null, false);
    serializer.configure(ImmutableMap.of(), false);
    return serializer;
  }

  private static Deserializer<?> getDeserializer(final SqlType colType) {
    final Deserializer<Object> deserializer = getSerdeSupplier(colType).getDeserializer(null, false);
    deserializer.configure(ImmutableMap.of(), false);
    return deserializer;
  }

  private static KafkaSerdeSupplier getSerdeSupplier(final SqlType colType) {
    return new KafkaSerdeSupplier(
        LogicalSchema.builder()
            .valueColumn(ColumnName.of("Look no key col"), colType)
            .build()
    );
  }
}
