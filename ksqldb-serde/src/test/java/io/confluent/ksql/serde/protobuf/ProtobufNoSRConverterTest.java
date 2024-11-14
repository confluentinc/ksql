/*
 * Copyright 2022 Confluent Inc.
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

package io.confluent.ksql.serde.protobuf;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Collections;

public class ProtobufNoSRConverterTest {

  @Test(expected = UnsupportedOperationException.class)
  public void shouldThrowExceptionWhenUsedWithNoArgConstructor1() {
    // Given
    final ProtobufNoSRConverter protobufNoSRConverter = new ProtobufNoSRConverter();

    // When
    protobufNoSRConverter.toConnectData("topic", "test".getBytes(StandardCharsets.UTF_8));
  }

  @Test(expected = UnsupportedOperationException.class)
  public void shouldThrowExceptionWhenUsedWithNoArgConstructor2() {
    // Given
    final ProtobufNoSRConverter protobufNoSRConverter = new ProtobufNoSRConverter();

    // When
    protobufNoSRConverter.fromConnectData("topic", Schema.STRING_SCHEMA, "test");
  }

  @Test
  public void shouldConvertFromSchemaAndValueWhenInstantiatedWithSchema() {
    // Given
    final ProtobufNoSRConverter protobufNoSRConverter =
        new ProtobufNoSRConverter(Schema.STRING_SCHEMA);
    protobufNoSRConverter.configure(Collections.emptyMap(), false);

    // When
    final byte[] bytes = protobufNoSRConverter.fromConnectData(
        "topic",
        Schema.STRING_SCHEMA,
        "test"
    );
    final SchemaAndValue schemaAndValue = protobufNoSRConverter.toConnectData("topic", bytes);

    //Then
    Assert.assertEquals(Schema.STRING_SCHEMA, schemaAndValue.schema());
    Assert.assertEquals("test", schemaAndValue.value());
  }
}