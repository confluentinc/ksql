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

import static io.confluent.connect.protobuf.ProtobufDataConfig.SCHEMAS_CACHE_SIZE_CONFIG;
import static io.confluent.connect.protobuf.ProtobufDataConfig.WRAPPER_FOR_RAW_PRIMITIVES_CONFIG;

import com.google.common.collect.ImmutableMap;
import io.confluent.connect.protobuf.ProtobufConverter;
import io.confluent.connect.protobuf.ProtobufData;
import io.confluent.connect.protobuf.ProtobufDataConfig;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.ksql.test.serde.ConnectSerdeSupplier;
import org.apache.kafka.connect.data.Schema;

public class ValueSpecProtobufSerdeSupplier extends ConnectSerdeSupplier<ProtobufSchema> {

  private final boolean unwrapPrimitives;

  public ValueSpecProtobufSerdeSupplier(final boolean unwrapPrimitives) {
    super(ProtobufConverter::new);
    this.unwrapPrimitives = unwrapPrimitives;
  }

  @Override
  protected Schema fromParsedSchema(final ProtobufSchema schema) {
    return new ProtobufData(new ProtobufDataConfig(ImmutableMap.of(
        SCHEMAS_CACHE_SIZE_CONFIG, 1,
        WRAPPER_FOR_RAW_PRIMITIVES_CONFIG, unwrapPrimitives
    ))).toConnectSchema(schema);
  }
}
