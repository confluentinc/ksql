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

package io.confluent.ksql.engine;

import io.confluent.ksql.config.ImmutableProperties;
import io.confluent.ksql.rest.entity.PropertiesList;

import java.util.Map;
import java.util.stream.Collectors;

/**
 * Utility class for dealing with engine properties
 */
final class KsqlEngineProps {

  static void throwOnImmutableOverride(final Map<String, Object> overriddenProperties) {
    final String immutableProps = overriddenProperties.keySet().stream()
        .filter(ImmutableProperties.getImmutableProperties()::contains)
        .distinct()
        .collect(Collectors.joining(","));

    if (!immutableProps.isEmpty()) {
      throw new IllegalArgumentException("Cannot override properties: " + immutableProps);
    }
  }

  static void throwOnNonQueryLevelConfigs(final Map<String, Object> overriddenProperties) {
    final String nonQueryLevelConfigs = overriddenProperties.keySet().stream()
        .filter(s -> !PropertiesList.QueryLevelPropertyList.contains(s))
        .distinct()
        .collect(Collectors.joining(","));

    if (!nonQueryLevelConfigs.isEmpty()) {
      throw new IllegalArgumentException("Cannot override properties: " + nonQueryLevelConfigs);
    }
  }

  private KsqlEngineProps() {
  }
}
