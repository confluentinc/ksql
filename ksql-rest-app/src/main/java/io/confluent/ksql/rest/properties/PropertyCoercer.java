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

package io.confluent.ksql.rest.properties;

import io.confluent.ksql.config.PropertyParser;
import io.confluent.ksql.properties.LocalPropertyParser;
import io.confluent.ksql.rest.entity.KsqlRequest;
import io.confluent.ksql.util.KsqlException;
import java.util.HashMap;
import java.util.Map;

/**
 * Used to coerce the types of properties supplied over text, (i.e. JSON REST API), to correct
 * type.
 */
public final class PropertyCoercer {

  private static final PropertyParser PROPERTY_PARSER = new LocalPropertyParser();

  private PropertyCoercer() {
  }

  public static Map<String, Object> coerceTypes(final KsqlRequest request) {
    final Map<String, ?> streamsProperties = request.getStreamsProperties();
    final Map<String, Object> validated = new HashMap<>(streamsProperties.size());
    streamsProperties.forEach((k, v) -> validated.put(k, coerceType(k, v)));
    return validated;
  }

  private static Object coerceType(final String key, final Object value) {
    try {
      final String stringValue = value == null ? null : String.valueOf(value);
      return PROPERTY_PARSER.parse(key, stringValue);
    } catch (final Exception e) {
      throw new KsqlException("Failed to set '" + key + "' to '" + value + "'", e);
    }
  }

}
