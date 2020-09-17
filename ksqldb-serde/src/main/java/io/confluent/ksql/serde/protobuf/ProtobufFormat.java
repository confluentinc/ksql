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

package io.confluent.ksql.serde.protobuf;

import com.google.common.collect.ImmutableMap;
import io.confluent.connect.protobuf.ProtobufData;
import io.confluent.connect.protobuf.ProtobufDataConfig;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.SerdeFeature;
import io.confluent.ksql.serde.connect.ConnectFormat;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Schema;

public class ProtobufFormat extends ConnectFormat {

  public static final String NAME = ProtobufSchema.TYPE;

  private final ProtobufData protobufData =
      new ProtobufData(new ProtobufDataConfig(ImmutableMap.of()));

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public Set<SerdeFeature> supportedFeatures() {
    return ProtobufSerdeFactory.SUPPORTED_FEATURES;
  }

  @Override
  protected <T> Serde<T> getConnectSerde(
      final ConnectSchema connectSchema,
      final Map<String, String> formatProps,
      final KsqlConfig config,
      final Supplier<SchemaRegistryClient> srFactory,
      final Class<T> targetType
  ) {
    return ProtobufSerdeFactory.createSerde(connectSchema, config, srFactory, targetType);
  }

  @Override
  protected Schema toConnectSchema(final ParsedSchema schema) {
    return protobufData.toConnectSchema((ProtobufSchema) schema);
  }

  @Override
  protected ParsedSchema fromConnectSchema(final Schema schema,
      final FormatInfo formatInfo) {
    // Bug in ProtobufData means `fromConnectSchema` throws on the second invocation if using
    // default naming.
    return new ProtobufData().fromConnectSchema(schema);
  }
}
