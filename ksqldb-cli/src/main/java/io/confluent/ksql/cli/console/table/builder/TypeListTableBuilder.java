/*
 * Copyright 2021 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
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
import io.confluent.ksql.rest.entity.TypeList;
import java.util.Comparator;
import java.util.List;
import java.util.Map.Entry;

public class TypeListTableBuilder implements TableBuilder<TypeList> {

  private static final List<String> HEADERS = ImmutableList.of("Type Name", "Schema");

  @Override
  public Table buildTable(final TypeList entity) {
    return new Table.Builder()
        .withColumnHeaders(HEADERS)
        .withRows(entity
            .getTypes()
            .entrySet()
            .stream()
            .sorted(Comparator.comparing(Entry::getKey))
            .map(entry -> ImmutableList.of(entry.getKey(), entry.getValue().toTypeString())))
        .build();
  }
}
