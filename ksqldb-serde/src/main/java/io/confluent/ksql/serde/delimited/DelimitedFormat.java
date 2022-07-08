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

package io.confluent.ksql.serde.delimited;

import com.google.common.collect.ImmutableSet;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.schema.ksql.PersistenceSchema;
import io.confluent.ksql.schema.ksql.types.SqlPrimitiveType;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.serde.Delimiter;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.serde.FormatProperties;
import io.confluent.ksql.serde.SerdeFeature;
import io.confluent.ksql.serde.SerdeUtils;
import io.confluent.ksql.util.KsqlConfig;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import org.apache.commons.csv.CSVFormat;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

public final class DelimitedFormat implements Format {

  private static final ImmutableSet<SerdeFeature> SUPPORTED_FEATURES = ImmutableSet.of(
      SerdeFeature.UNWRAP_SINGLES
  );

  private static final String DEFAULT_DELIMITER = ",";

  public static final String DELIMITER = "delimiter";
  public static final String NAME = "DELIMITED";

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
    return ImmutableSet.of(DELIMITER);
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

    final Delimiter delimiter = getDelimiter(formatProperties);

    final CSVFormat csvFormat = CSVFormat.DEFAULT.withDelimiter(delimiter.getDelimiter());

    return Serdes.serdeFrom(
        new KsqlDelimitedSerializer(schema, csvFormat),
        new KsqlDelimitedDeserializer(schema, csvFormat)
    );
  }

  private static Delimiter getDelimiter(final Map<String, String> formatProperties) {
    return Delimiter.parse(formatProperties.getOrDefault(DELIMITER, DEFAULT_DELIMITER));
  }

  @Override
  public boolean supportsKeyType(final SqlType type) {
    return type instanceof SqlPrimitiveType;
  }
}
