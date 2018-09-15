/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.ksql.cli.console.table.builder;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.cli.console.table.Table;
import io.confluent.ksql.cli.console.table.Table.Builder;
import io.confluent.ksql.rest.entity.PropertiesList;
import io.confluent.ksql.util.CliUtils;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class PropertiesListTableBuilder implements TableBuilder<PropertiesList> {

  private static final List<String> HEADERS =
      ImmutableList.of("Property", "Value");

  @Override
  public Table buildTable(final PropertiesList entity) {
    return print(CliUtils.propertiesListWithOverrides(entity))
        .build();
  }

  public Table.Builder print(final Map<String, Object> properties) {
    return new Builder()
        .withColumnHeaders(HEADERS)
        .withRows(propertiesRowValues(properties));
  }

  private static List<List<String>> propertiesRowValues(final Map<String, Object> properties) {
    return properties.entrySet().stream()
        .sorted(Map.Entry.comparingByKey())
        .map(
            propertyEntry -> Arrays.asList(
                propertyEntry.getKey(),
                Objects.toString(propertyEntry.getValue())
            ))
        .collect(Collectors.toList());
  }
}
