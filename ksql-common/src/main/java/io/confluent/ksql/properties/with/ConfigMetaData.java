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

package io.confluent.ksql.properties.with;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.configdef.UnmodifiableConfigDef;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.common.config.ConfigDef;

/**
 * Holds metadata about config defs.
 */
@Immutable
public final class ConfigMetaData {

  private final UnmodifiableConfigDef configDef;
  private final Set<String> configNames;
  private final List<String> orderedConfigNames;
  private final Set<String> shortConfigs;

  static ConfigMetaData of(final ConfigDef configDef) {
    return new ConfigMetaData(configDef);
  }

  private ConfigMetaData(final ConfigDef configDef) {
    this.configDef = UnmodifiableConfigDef.of(Objects.requireNonNull(configDef, "configDef"));
    this.configNames = configNames(configDef);
    this.orderedConfigNames = orderedConfigNames(configNames);
    this.shortConfigs = configsOfTypeShort(configDef);
  }

  public UnmodifiableConfigDef getConfigDef() {
    return configDef;
  }

  public Set<String> getConfigNames() {
    return configNames;
  }

  public List<String> getOrderedConfigNames() {
    return orderedConfigNames;
  }

  public Set<String> getShortConfigs() {
    return shortConfigs;
  }

  private static Set<String> configNames(final ConfigDef configDef) {
    return configDef.names().stream()
        .map(String::toUpperCase)
        .collect(Collectors.toSet());
  }

  private static List<String> orderedConfigNames(final Set<String> configNames) {
    final List<String> names = new ArrayList<>(configNames);
    names.sort(Comparator.naturalOrder());
    return ImmutableList.copyOf(names);
  }

  private static Set<String> configsOfTypeShort(final ConfigDef configDef) {
    return ImmutableSet.copyOf(
        configDef.configKeys().entrySet().stream()
            .filter(e -> e.getValue().type() == ConfigDef.Type.SHORT)
            .map(Entry::getKey)
            .map(String::toUpperCase)
            .collect(Collectors.toSet())
    );
  }
}
