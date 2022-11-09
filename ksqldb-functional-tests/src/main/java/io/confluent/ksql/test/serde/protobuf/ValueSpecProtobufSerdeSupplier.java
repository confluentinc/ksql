/*
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.test.serde.protobuf;

import static io.confluent.connect.protobuf.ProtobufDataConfig.OPTIONAL_FOR_NULLABLES_CONFIG;
import static io.confluent.connect.protobuf.ProtobufDataConfig.WRAPPER_FOR_NULLABLES_CONFIG;

import com.google.common.collect.ImmutableMap;
import io.confluent.connect.protobuf.ProtobufConverter;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.ksql.serde.protobuf.ProtobufProperties;
import io.confluent.ksql.serde.protobuf.ProtobufSchemaTranslator;
import io.confluent.ksql.test.serde.ConnectSerdeSupplier;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.storage.Converter;

public class ValueSpecProtobufSerdeSupplier extends ConnectSerdeSupplier<ProtobufSchema> {

  private final ProtobufSchemaTranslator schemaTranslator;

  private final ImmutableMap<String, Boolean> converterConfig;

  public ValueSpecProtobufSerdeSupplier(final ProtobufProperties protobufProperties) {
    super(ProtobufConverter::new);
    this.schemaTranslator = new ProtobufSchemaTranslator(protobufProperties);
    this.converterConfig = ImmutableMap.of(
        OPTIONAL_FOR_NULLABLES_CONFIG, protobufProperties.isNullableAsOptional(),
        WRAPPER_FOR_NULLABLES_CONFIG, protobufProperties.isNullableAsWrapper()
    );
  }

  @Override
  protected Schema fromParsedSchema(final ProtobufSchema schema) {
    return schemaTranslator.toConnectSchema(schema);
  }

  @Override
  protected void configureConverter(final Converter c, final boolean isKey) {
    c.configure(
        ImmutableMap.<String, Object>builder()
            .putAll(converterConfig)
            .put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "foo")
            .build(),
        isKey
    );
  }

}
