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
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
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
  private final ImmutableSet<String> configNames;
  private final ImmutableList<String> orderedConfigNames;
  private final ImmutableSet<String> shortConfigs;

  static ConfigMetaData of(final ConfigDef configDef) {
    return new ConfigMetaData(configDef);
  }

  private ConfigMetaData(final ConfigDef configDef) {
    this.configDef = UnmodifiableConfigDef.of(Objects.requireNonNull(configDef, "configDef"));
    this.configNames = configNames(configDef);
    this.orderedConfigNames = orderedConfigNames(configNames);
    this.shortConfigs = configsOfTypeShort(configDef);
  }

  @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "configDef is UnmodifiableConfigDef")
  public UnmodifiableConfigDef getConfigDef() {
    return configDef;
  }

  @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "configNames is ImmutableSet")
  public Set<String> getConfigNames() {
    return configNames;
  }

  @SuppressFBWarnings(
      value = "EI_EXPOSE_REP",
      justification = "orderedConfigNames is ImmutableList"
  )
  public List<String> getOrderedConfigNames() {
    return orderedConfigNames;
  }

  @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "shortConfigs is ImmutableSet")
  public Set<String> getShortConfigs() {
    return shortConfigs;
  }

  private static ImmutableSet<String> configNames(final ConfigDef configDef) {
    return ImmutableSet.copyOf(configDef.names().stream()
        .map(String::toUpperCase)
        .collect(Collectors.toSet()));
  }

  private static ImmutableList<String> orderedConfigNames(final Set<String> configNames) {
    final List<String> names = new ArrayList<>(configNames);
    names.sort(Comparator.naturalOrder());
    return ImmutableList.copyOf(names);
  }

  private static ImmutableSet<String> configsOfTypeShort(final ConfigDef configDef) {
    return ImmutableSet.copyOf(
        configDef.configKeys().entrySet().stream()
            .filter(e -> e.getValue().type() == ConfigDef.Type.SHORT)
            .map(Entry::getKey)
            .map(String::toUpperCase)
            .collect(Collectors.toSet())
    );
  }
}
