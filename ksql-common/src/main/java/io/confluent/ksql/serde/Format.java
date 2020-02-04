/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.serde;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;
import io.confluent.ksql.util.KsqlException;
import java.util.Map;
import java.util.Set;

public enum Format {

  JSON(true, ImmutableSet.of(), ImmutableSet.of()),
  AVRO(true, ImmutableSet.of(FormatInfo.FULL_SCHEMA_NAME), ImmutableSet.of()),
  DELIMITED(false, ImmutableSet.of(FormatInfo.DELIMITER), ImmutableSet.of(FormatInfo.DELIMITER)),
  KAFKA(false, ImmutableSet.of(), ImmutableSet.of());

  private final boolean supportsUnwrapping;
  private final Set<String> validConfigs;
  private final Set<String> inheritableProperties;

  Format(
      final boolean supportsUnwrapping,
      final Set<String> validConfigs,
      final Set<String> inheritableProperties
  ) {
    this.supportsUnwrapping = supportsUnwrapping;
    this.validConfigs = validConfigs;
    this.inheritableProperties = inheritableProperties;
  }

  /**
   * If this format supports unwrapping, primitive values can optionally
   * be serialized anonymously (i.e. without a wrapping STRUCT and
   * corresponding field name)
   *
   * @return whether or not this format supports unwrapping
   */
  public boolean supportsUnwrapping() {
    return supportsUnwrapping;
  }

  /**
   * @param properties the properties to validate
   * @throws KsqlException if the properties are invalid for the given format
   */
  public void validateProperties(final Map<String, String> properties) {
    final SetView<String> difference = Sets.difference(properties.keySet(), validConfigs);
    if (!difference.isEmpty()) {
      throw new KsqlException(name() + " does not support the following configs: " + difference);
    }

    properties.forEach((k, v) -> {
      if (v.trim().isEmpty()) {
        throw new KsqlException(k + " cannot be empty. Format configuration: " + properties);
      }
    });
  }

  /**
   * Specifies the set of "inheritable" properties - these properties will
   * persist across streams and tables if a sink is created from a source
   * of the same type and does not explicitly overwrite the property.
   *
   * <p>For example, if a stream with format {@code DELIMITED} was created
   * with {@code VALUE_DELIMITER='x'}, any {@code DELIMITED} sinks that are
   * created with that stream as its source will also have the same delimiter
   * value of {@code x}</p>
   *
   * @return the set of properties that are considered "inheritable"
   */
  public Set<String> getInheritableProperties() {
    return inheritableProperties;
  }

  public static Format of(final String value) {
    try {
      return valueOf(value.toUpperCase());
    } catch (final IllegalArgumentException e) {
      throw new KsqlException("Unknown format: " + value);
    }
  }
}
