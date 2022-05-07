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

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Collections;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.storage.Converter;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@SuppressWarnings("checkstyle:ClassDataAbstractionCoupling")
@RunWith(MockitoJUnitRunner.class)
public class KsqlProtobufNoSRDeserializerTest extends  AbstractKsqlProtobufDeserializerTest {

  @Override
  Converter getConverter(final ConnectSchema schema) {
    final ProtobufNoSRConverter converter = new ProtobufNoSRConverter(schema);
    converter.configure(Collections.emptyMap(), false);
    return converter;
  }

  @Override
  byte[] givenConnectSerialized(
      final Converter converter,
      final Object value,
      final Schema connectSchema
  ) {
    return converter.fromConnectData(SOME_TOPIC, connectSchema, value);
  }

  @Override
  <T> Deserializer<T> givenDeserializerForSchema(
      final ConnectSchema schema,
      final Class<T> targetType
  ) {
    final Deserializer<T> deserializer = new ProtobufNoSRSerdeFactory(ImmutableMap.of())
        .createSerde(
            schema,
            new KsqlConfig(ImmutableMap.of()),
            () -> null,
            targetType,
            false).deserializer();

    deserializer.configure(Collections.emptyMap(), false);

    return deserializer;
  }
}
