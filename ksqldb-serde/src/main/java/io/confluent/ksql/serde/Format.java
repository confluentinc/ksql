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

package io.confluent.ksql.serde;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.schema.ksql.PersistenceSchema;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.kafka.common.serialization.Serde;

/**
 * A {@code Format} is a serialization specification of a Kafka topic
 * in ksqlDB. The builtin formats are specified in the {@link FormatFactory}
 * class.
 *
 * @apiNote implementations are expected to be Thread Safe
 */
@ThreadSafe
public interface Format {

  /**
   * The name of the {@code Format} specification.
   *
   * <p>As used for {@code FORMAT}, {@code VALUE_FORMAT} and {@code KEY_FORMAT} properties in SQL
   * statements.
   *
   * @return the name of this Format
   */
  String name();

  /**
   * @return The set of features the format supports.
   */
  default Set<SerdeFeature> supportedFeatures() {
    return ImmutableSet.of();
  }

  /**
   * @param feature the feature to test
   * @return {@code true} if the feature is supported
   */
  default boolean supportsFeature(final SerdeFeature feature) {
    return supportedFeatures().contains(feature);
  }

  /**
   * Get a type for converting between {@link ParsedSchema} returned by Confluent Schema Registry
   * and ksqlDB's own schema types.
   *
   * <p>If this Format supports the {@link SerdeFeature#SCHEMA_INFERENCE} feature, it is expected
   * that this method will be implemented.
   *
   * @param formatProperties any format specific properties
   * @return the converter
   * @see SerdeFeature#SCHEMA_INFERENCE
   */
  default SchemaTranslator getSchemaTranslator(Map<String, String> formatProperties) {
    throw new UnsupportedOperationException(name() + " does not implement Schema Registry support");
  }

  /**
   * Get a type for converting between {@link ParsedSchema} returned by Confluent Schema Registry
   * and ksqlDB's own schema types.
   *
   * <p>If this Format supports the {@link SerdeFeature#SCHEMA_INFERENCE} feature, it is expected
   * that this method will be implemented.
   *
   * @param formatProperties any format specific properties
   * @param policies controls how schema should be translated
   * @return the converter
   * @see SerdeFeature#SCHEMA_INFERENCE
   */
  default SchemaTranslator getSchemaTranslator(final Map<String, String> formatProperties,
      final SchemaTranslationPolicies policies) {
    throw new UnsupportedOperationException(name() + " does not implement Schema Registry support");
  }

  /**
   * If the format accepts custom properties in the WITH clause of the statement, then this will
   * take the properties and validate the key-value pairs.
   *
   * @param properties the properties to validate
   * @throws KsqlException if the properties are invalid for the given format
   */
  default void validateProperties(final Map<String, String> properties) {
    // by default, this method ensures that there are no property names
    // (case-insensitive) that are not in the getSupportedProperties()
    // and that none of the values are empty
    FormatProperties.validateProperties(name(), properties, getSupportedProperties());
  }

  /**
   * If the format accepts custom properties in the WITH clause of the statement,
   * this method dictates which property keys are valid keys. This is used in
   * conjunction with {@link #validateProperties(Map)} to ensure that the format
   * configuration is valid.
   *
   * @return a set of valid property names
   */
  default Set<String> getSupportedProperties() {
    return ImmutableSet.of();
  }

  /**
   * Specifies the set of "inheritable" properties - these properties will
   * persist across streams and tables if a sink is created from a source
   * of the same format and does not explicitly overwrite the property.
   *
   * <p>For example, if a stream with format {@code DELIMITED} was created
   * with {@code VALUE_DELIMITER='x'}, any {@code DELIMITED} sinks that are
   * created with that stream as its source will also have the same delimiter
   * value of {@code x}</p>
   *
   * <p>The default implementation of this method assumes that all of the
   * supported properties are inheritable.</p>
   *
   * @return the set of properties that are considered "inheritable"
   */
  default Set<String> getInheritableProperties() {
    return getSupportedProperties();
  }

  /**
   * Get the serde for the supplied {@code schema}.
   *
   * @param schema the schema of the data
   * @param formatProperties any format specific properties
   * @param ksqlConfig the session config
   * @param srClientFactory supplier of the SR client
   * @param isKey whether or not we're retreiving a key serde
   * @return a serde pair capable of (de)serializing the data in this format.
   */
  Serde<List<?>> getSerde(
      PersistenceSchema schema,
      Map<String, String> formatProperties,
      KsqlConfig ksqlConfig,
      Supplier<SchemaRegistryClient> srClientFactory,
      boolean isKey
  );

  /**
   * Check whether given sql type is supported by this format.
   *
   * @param type given sql key type
   * @return true if the given sql type is supported
   */
  boolean supportsKeyType(SqlType type);

  /**
   * Returns a list of schema names found in the {@code ParsedSchema}.
   * </p>
   * It may return an empty list if not names are found. Names are not found ont formats
   * such as Delimited and Json (no SR) formats.
   */
  default List<String> schemaFullNames(final ParsedSchema schema) {
    return ImmutableList.of();
  }
}
