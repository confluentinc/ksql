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

package io.confluent.ksql.cli.console.table.builder;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.cli.console.table.Table;
import io.confluent.ksql.rest.entity.VariablesList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public class ListVariablesTableBuilder implements TableBuilder<VariablesList> {
  private static final List<String> HEADERS =
      ImmutableList.of("Variable Name", "Value");

  @Override
  public Table buildTable(final VariablesList entity) {
    return new Table.Builder()
        .withColumnHeaders(HEADERS)
        .withRows(defRowValues(entity.getVariables()))
        .build();
  }

  private static List<List<String>> defRowValues(final List<VariablesList.Variable> variables) {
    return variables.stream()
        .sorted(Comparator.comparing(var -> var.getName()))
        .map(var -> ImmutableList.of(
            var.getName(), var.getValue()))
        .collect(Collectors.toList());
  }
}
