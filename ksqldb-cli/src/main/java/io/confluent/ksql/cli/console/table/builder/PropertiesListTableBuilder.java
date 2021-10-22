/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.cli.console.table.builder;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.cli.console.table.Table;
import io.confluent.ksql.cli.console.table.Table.Builder;
import io.confluent.ksql.rest.entity.PropertiesList;
import io.confluent.ksql.rest.entity.PropertiesList.Property;
import io.confluent.ksql.util.InternalKsqlConfig;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

public class PropertiesListTableBuilder implements TableBuilder<PropertiesList> {

  private static final List<String> HEADERS =
      ImmutableList.of("Property", "Scope", "Level", "Default override", "Effective Value");

  @Override
  public Table buildTable(final PropertiesList entity) {
    return new Builder()
        .withColumnHeaders(HEADERS)
        .withRows(defRowValues(propertiesListWithOverrides(entity)))
        .build();
  }

  private static List<List<String>> defRowValues(final List<PropertyDef> properties) {
    return properties.stream()
        .sorted(Comparator.comparing(propertyDef -> propertyDef.propertyName))
        .map(def -> ImmutableList.of(
            def.propertyName, def.scope, def.overrideType, def.effectiveValue))
        .collect(Collectors.toList());
  }

  private static List<PropertyDef> propertiesListWithOverrides(final PropertiesList properties) {

    final Function<Property, PropertyDef> toPropertyDef = property -> {
      final String name = property.getName();
      // Filter out internal configs
      if (InternalKsqlConfig.CONFIG_DEF.names().contains(name)) {
        return null;
      }

      final String level;
      if (KsqlConfig.CURRENT_DEF.names().contains(name) || KsqlConfig.LEGACY_DEF.names().contains(name)) {
        // LEGACY_DEF and CURRENT_DEF should differ only by default values, but check them both for future-proofness
        level = KsqlConfig.CURRENT_DEF.configKeys().get(name).group;
      } else {
        // all configs are server-level by default, unless explicitly handled per query
        level = "SERVER";
        // TODO -- what are these configs? maybe streams/producer configs? only the overridden ones, or others? test
      }
      final String value = property.getValue() == null ? "NULL" : property.getValue();
      final String scope = property.getScope();

      if (properties.getOverwrittenProperties().contains(name)) {
        return new PropertyDef(name, scope, "SESSION", value);
      }

      if (properties.getDefaultProperties().contains(name)) {
        return new PropertyDef(name, scope, "", value);
      }

      return new PropertyDef(name, scope, "SERVER", value);
    };

    return properties.getProperties().stream()
        .map(toPropertyDef)
        .collect(Collectors.toList());
  }

  private static class PropertyDef {

    private final String propertyName;
    private final String scope;
    private final String overrideType;
    private final String effectiveValue;

    PropertyDef(
        final String propertyName,
        final String scope,
        final String overrideType,
        final String effectiveValue) {
      this.propertyName = Objects.requireNonNull(propertyName, "propertyName");
      this.scope = Objects.requireNonNull(scope, "scope");
      this.overrideType = Objects.requireNonNull(overrideType, "overrideType");
      this.effectiveValue = Objects.requireNonNull(effectiveValue, "effectiveValue");
    }
  }
}
