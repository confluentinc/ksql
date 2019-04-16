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
import io.confluent.ksql.util.CliUtils;
import io.confluent.ksql.util.CliUtils.PropertyDef;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public class PropertiesListTableBuilder implements TableBuilder<PropertiesList> {

  private static final List<String> HEADERS =
      ImmutableList.of("Property", "Default override", "Effective Value");

  @Override
  public Table buildTable(final PropertiesList entity) {
    return new Builder()
        .withColumnHeaders(HEADERS)
        .withRows(defRowValues(CliUtils.propertiesListWithOverrides(entity)))
        .build();
  }

  private static List<List<String>> defRowValues(final List<PropertyDef> properties) {
    return properties.stream()
        .sorted(Comparator.comparing(PropertyDef::getPropertyName))
        .map(def ->
            ImmutableList.of(def.getPropertyName(), def.getOverrideType(), def.getEffectiveValue()))
        .collect(Collectors.toList());
  }
}
