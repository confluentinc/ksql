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

package io.confluent.ksql.serde;

import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;
import io.confluent.ksql.util.KsqlException;
import java.util.Map;
import java.util.Set;

/**
 * Util class for handling format properties
 */
public final class FormatProperties {

  private FormatProperties() {
  }

  /**
   * Helper to validate the supplied {@code properties} against a {@link
   * Format#getSupportedProperties() format's supported properties}.
   *
   * @param formatName the name of the format - only used in error messages
   * @param properties the properties the user supplied
   * @param supportedProperties the formats supported properties
   * @throws KsqlException on unknown keys or empty values.
   */
  public static void validateProperties(
      final String formatName,
      final Map<String, String> properties,
      final Set<String> supportedProperties
  ) {
    final SetView<String> diff = Sets.difference(properties.keySet(), supportedProperties);
    if (!diff.isEmpty()) {
      throw new KsqlException(formatName + " does not support the following configs: " + diff);
    }

    properties.forEach((k, v) -> {
      if (v.trim().isEmpty()) {
        throw new KsqlException(k + " cannot be empty. Format configuration: " + properties);
      }
    });
  }
}
