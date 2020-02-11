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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.ksql.util.KsqlException;
import java.util.Map;
import java.util.Set;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.kafka.connect.data.Schema;

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
   * The name of the {@code Format} specification. If this format supports
   * Confluent Schema Registry integration (either builtin or custom via the
   * {@code ParsedSchema} plugin support), this should match the value returned
   * by {@link ParsedSchema#name()}. Note that this value is <i>case-sensitive</i>.
   *
   * @return the name of this Format
   * @see #supportsSchemaInference()
   */
  String name();

  /**
   * If this format supports wrapping, primitive values can optionally
   * be serialized anonymously (i.e. without a wrapping STRUCT and
   * corresponding field name) - otherwise primitive values are only
   * represented anonymously.
   *
   * @return whether or not this format supports wrapping
   */
  default boolean supportsWrapping() {
    return true;
  }

  /**
   * Indicates whether or not this format can support CREATE statements that
   * omit the table elements and instead determine the schema from a Confluent
   * Schema Registry query. If this method returns {@code true}, it is expected
   * that the {@link #name()} corresponds with the schema format name returned
   * by {@link ParsedSchema#name()} for this format.
   *
   * @return {@code true} if this {@code Format} supports schema inference
   *         through Confluent Schema Registry
   */
  default boolean supportsSchemaInference() {
    return false;
  }

  /**
   * Converts the {@link ParsedSchema} returned by Confluent Schema Registry
   * into a Connect Schema, which ksqlDB can use to infer the stream or table
   * schema.
   *
   * <p>If this Format {@link #supportsSchemaInference()}, it is expected that
   * this method will be implemented.</p>
   *
   * @param schema the {@code ParsedSchema} returned from Schema Registry
   * @return the corresponding Kafka Connect schema for the {@code schema} param
   */
  default Schema toConnectSchema(ParsedSchema schema) {
    throw new KsqlException("Format does not implement Schema Registry support: " + name());
  }

  default ParsedSchema toParsedSchema(Schema schema) {
    throw new KsqlException("Format does not implement Schema Registry support: " + name());
  }

  /**
   * If the format accepts custom properties in the WITH clause of the statement,
   * then this will take the properties and validate the key-value pairs.
   *
   * @param properties the properties to validate
   * @throws KsqlException if the properties are invalid for the given format
   */
  default void validateProperties(Map<String, String> properties) {
    // by default, this method ensures that there are no property names
    // (case-insensitive) that are not in the getSupportedProperties()
    // and that none of the values are empty
    final SetView<String> diff = Sets.difference(properties.keySet(), getSupportedProperties());
    if (!diff.isEmpty()) {
      throw new KsqlException(name() + " does not support the following configs: " + diff);
    }

    properties.forEach((k, v) -> {
      if (v.trim().isEmpty()) {
        throw new KsqlException(k + " cannot be empty. Format configuration: " + properties);
      }
    });
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
   * @param info the info containing information required for generating the factory
   * @return a {@code KsqlSerdeFactory} that generates serdes for the given format
   */
  KsqlSerdeFactory getSerdeFactory(FormatInfo info);
}
