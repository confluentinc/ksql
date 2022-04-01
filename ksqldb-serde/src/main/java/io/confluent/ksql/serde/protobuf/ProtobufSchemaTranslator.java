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

package io.confluent.ksql.serde.protobuf;

import static io.confluent.connect.protobuf.ProtobufDataConfig.WRAPPER_FOR_RAW_PRIMITIVES_CONFIG;

import com.google.common.collect.ImmutableMap;
import io.confluent.connect.protobuf.ProtobufData;
import io.confluent.connect.protobuf.ProtobufDataConfig;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.ksql.serde.connect.ConnectSchemaTranslator;
import java.util.Map;
import java.util.Objects;
import org.apache.kafka.connect.data.Schema;

/**
 * Translates between Connect and Protobuf Schema Registry schema types.
 */
class ProtobufSchemaTranslator implements ConnectSchemaTranslator {

  private final Map<String, Object> protobufDataConfig;
  private final ProtobufData protobufData;

  ProtobufSchemaTranslator(final ProtobufProperties properties) {
    Objects.requireNonNull(properties, "properties");
    this.protobufDataConfig = ImmutableMap.of(
        WRAPPER_FOR_RAW_PRIMITIVES_CONFIG, properties.getUnwrapPrimitives());

    this.protobufData = new ProtobufData(new ProtobufDataConfig(protobufDataConfig));
  }

  @Override
  public String name() {
    return ProtobufSchema.TYPE;
  }

  @Override
  public Schema toConnectSchema(final ParsedSchema schema) {
    return protobufData.toConnectSchema((ProtobufSchema) schema);
  }

  @Override
  public ParsedSchema fromConnectSchema(final Schema schema) {
    // Bug in ProtobufData means `fromConnectSchema` throws on the second invocation if using
    // default naming.
    return new ProtobufData(new ProtobufDataConfig(protobufDataConfig)).fromConnectSchema(schema);
  }
}
