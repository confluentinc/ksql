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

package io.confluent.ksql.serde.none;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.schema.ksql.PersistenceSchema;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.serde.FormatProperties;
import io.confluent.ksql.serde.SerdeUtils;
import io.confluent.ksql.serde.voids.KsqlVoidSerde;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.kafka.common.serialization.Serde;

/**
 * Format used to indicate no data.
 */
public class NoneFormat implements Format {

  public static final String NAME = "NONE";

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public Serde<List<?>> getSerde(
      final PersistenceSchema schema,
      final Map<String, String> formatProperties,
      final KsqlConfig ksqlConfig,
      final Supplier<SchemaRegistryClient> srClientFactory,
      final boolean isKey) {
    FormatProperties.validateProperties(name(), formatProperties, getSupportedProperties());
    SerdeUtils.throwOnUnsupportedFeatures(schema.features(), supportedFeatures());

    if (!schema.columns().isEmpty()) {
      throw new KsqlException("The '" + NAME
          + "' format can only be used when no columns are defined. Got: " + schema.columns());
    }

    return new KsqlVoidSerde<>();
  }
}
