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

package io.confluent.ksql.serde.avro;

import com.google.common.base.CaseFormat;
import com.google.common.collect.ImmutableSet;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.serde.SerdeFeature;
import io.confluent.ksql.serde.connect.ConnectFormat;
import io.confluent.ksql.serde.connect.ConnectSchemaTranslator;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.connect.data.ConnectSchema;

public final class AvroFormat extends ConnectFormat {

  private static final ImmutableSet<SerdeFeature> SUPPORTED_FEATURES = ImmutableSet.of(
      SerdeFeature.SCHEMA_INFERENCE,
      SerdeFeature.WRAP_SINGLES,
      SerdeFeature.UNWRAP_SINGLES
  );

  public static final String NAME = "AVRO";

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
    return AvroProperties.SUPPORTED_PROPERTIES;
  }

  @Override
  public Set<String> getInheritableProperties() {
    return ImmutableSet.of();
  }

  @Override
  protected ConnectSchemaTranslator getConnectSchemaTranslator(
      final Map<String, String> formatProps
  ) {
    return new AvroSchemaTranslator(new AvroProperties(formatProps));
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
    // Ensure schema is compatible by converting to Avro schema:
    getConnectSchemaTranslator(formatProps).fromConnectSchema(connectSchema);

    return new KsqlAvroSerdeFactory(new AvroProperties(formatProps))
        .createSerde(connectSchema, config, srFactory, targetType, isKey);
  }

  public static String getKeySchemaName(final String name) {
    final String camelName = CaseFormat.UPPER_UNDERSCORE
        .converterTo(CaseFormat.UPPER_CAMEL)
        .convert(name);
    return AvroProperties.AVRO_SCHEMA_NAMESPACE + "." + camelName + "Key";
  }
}
