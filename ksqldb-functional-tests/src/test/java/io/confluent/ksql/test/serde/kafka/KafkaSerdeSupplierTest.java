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
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.Test;

public class KafkaSerdeSupplierTest {

  @Test
  public void shouldSerializeKeyAsStringIfNoKeyColumnsToAllowTestsToProveKeyIsIgnored() {
    // Given:
    final KafkaSerdeSupplier supplier = new KafkaSerdeSupplier(
        LogicalSchema.builder()
            .valueColumn(ColumnName.of("Look no key col"), SqlTypes.STRING)
            .build()
    );

    final Serializer<Object> serializer = supplier.getSerializer(null);
    serializer.configure(ImmutableMap.of(), true);

    // When:
    final byte[] data = serializer.serialize("t", "22");

    // Then:
    assertThat(new String(data, UTF_8), is("22"));
  }

  @Test
  public void shouldDeserializeKeyAsStringIfNoKeyColumnsToAllowTestsToProveKeyIsIgnored() {
    // Given:
    final KafkaSerdeSupplier supplier = new KafkaSerdeSupplier(
        LogicalSchema.builder()
            .valueColumn(ColumnName.of("Look no key col"), SqlTypes.STRING)
            .build()
    );

    final Deserializer<Object> deserializer = supplier.getDeserializer(null);
    deserializer.configure(ImmutableMap.of(), true);

    // When:
    final Object result = deserializer.deserialize("t", "22".getBytes(UTF_8));

    // Then:
    assertThat(result, is("22"));
  }
}
