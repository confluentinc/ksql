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
import java.util.Comparator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

public class PropertiesListTableBuilder implements TableBuilder<PropertiesList> {

  private static final List<String> HEADERS =
      ImmutableList.of("Property", "Default override", "Effective Value");

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
        .map(
            def -> ImmutableList.of(def.propertyName, def.overrideType, def.effectiveValue))
        .collect(Collectors.toList());
  }

  private static List<PropertyDef> propertiesListWithOverrides(final PropertiesList properties) {

    final Function<Entry<String, ?>, PropertyDef> toPropertyDef = e -> {
      final String value = e.getValue() == null ? "NULL" : e.getValue().toString();
      if (properties.getOverwrittenProperties().contains(e.getKey())) {
        return new PropertyDef(e.getKey(), "SESSION", value);
      }

      if (properties.getDefaultProperties().contains(e.getKey())) {
        return new PropertyDef(e.getKey(), "", value);
      }

      return new PropertyDef(e.getKey(), "SERVER", value);
    };

    return properties.getProperties().entrySet().stream()
        .map(toPropertyDef)
        .collect(Collectors.toList());
  }

  private static class PropertyDef {

    private final String propertyName;
    private final String overrideType;
    private final String effectiveValue;

    PropertyDef(
        final String propertyName,
        final String overrideType,
        final String effectiveValue) {
      this.propertyName = Objects.requireNonNull(propertyName, "propertyName");
      this.overrideType = Objects.requireNonNull(overrideType, "overrideType");
      this.effectiveValue = Objects.requireNonNull(effectiveValue, "effectiveValue");
    }
  }
}
