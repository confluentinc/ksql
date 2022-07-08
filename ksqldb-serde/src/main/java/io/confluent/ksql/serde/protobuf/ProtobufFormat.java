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

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.squareup.wire.schema.internal.parser.ProtoFileElement;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.serde.FormatProperties;
import io.confluent.ksql.serde.SerdeFeature;
import io.confluent.ksql.serde.connect.ConnectFormat;
import io.confluent.ksql.serde.connect.ConnectSchemaTranslator;
import io.confluent.ksql.util.KsqlConfig;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.connect.data.ConnectSchema;

public class ProtobufFormat extends ConnectFormat {

  static final ImmutableSet<SerdeFeature> SUPPORTED_FEATURES = ImmutableSet.of(
      SerdeFeature.SCHEMA_INFERENCE,
      SerdeFeature.WRAP_SINGLES
  );

  public static final String NAME = "PROTOBUF";

  @Override
  public String name() {
    return NAME;
  }

  @Override
  @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "SUPPORTED_FEATURES is ImmutableSet")
  public Set<SerdeFeature> supportedFeatures() {
    return SUPPORTED_FEATURES;
  }


  @Override
  public Set<String> getSupportedProperties() {
    return ProtobufProperties.SUPPORTED_PROPERTIES;
  }

  @Override
  public Set<String> getInheritableProperties() {
    return ProtobufProperties.INHERITABLE_PROPERTIES;
  }

  @Override
  protected ConnectSchemaTranslator getConnectSchemaTranslator(
      final Map<String, String> formatProps
  ) {
    FormatProperties.validateProperties(name(), formatProps, getSupportedProperties());
    return new ProtobufSchemaTranslator(new ProtobufProperties(formatProps));
  }

  @Override
  protected <T> Serde<T> getConnectSerde(
      final ConnectSchema connectSchema,
      final Map<String, String> formatProps,
      final KsqlConfig config,
      final Supplier<SchemaRegistryClient> srFactory,
      final Class<T> targetType,
      final boolean isKey
  ) {
    return new ProtobufSerdeFactory(new ProtobufProperties(formatProps))
        .createSerde(connectSchema, config, srFactory, targetType, isKey);
  }

  @Override
  public List<String> schemaFullNames(final ParsedSchema schema) {
    if (schema.rawSchema() instanceof ProtoFileElement) {
      final ProtoFileElement protoFileElement = (ProtoFileElement) schema.rawSchema();
      final String packageName = protoFileElement.getPackageName();

      return protoFileElement.getTypes().stream()
          .map(typeElement -> Joiner.on(".").skipNulls().join(packageName, typeElement.getName()))
          .collect(Collectors.toList());
    }

    return ImmutableList.of();
  }
}
