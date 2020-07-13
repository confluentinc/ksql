/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the
 * License.
 */

package io.confluent.ksql.cli.console.table.builder;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.cli.console.table.Table;
import io.confluent.ksql.cli.console.table.Table.Builder;
import io.confluent.ksql.rest.entity.FunctionNameList;
import io.confluent.ksql.rest.entity.SimpleFunctionInfo;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class FunctionNameListTableBuilder implements TableBuilder<FunctionNameList> {

  private static final List<String> HEADERS = ImmutableList.of("Function Name", "Category");
  private static final List<String> EMPTY_ROW = ImmutableList.of(" ", " ");

  @Override
  public Table buildTable(final FunctionNameList functionNameList) {

    final Builder builder = new Builder().withColumnHeaders(HEADERS);

    // poor mans version check for the case we are running against an older ksqlDB server
    final Iterator<SimpleFunctionInfo> funcs = functionNameList.getFunctions().iterator();
    if (!funcs.hasNext() || funcs.next().getCategory().isEmpty()) {
      final Stream<List<String>> rows = functionNameList.getFunctions()
          .stream()
          .sorted(Comparator.comparing(SimpleFunctionInfo::getName))
          .map(func -> ImmutableList.of(func.getName(), func.getType().name().toUpperCase()));
      builder.withRows(rows);
    } else { // category info present, use new display layout
      final List<SimpleFunctionInfo> sortedFunctions = functionNameList.getFunctions().stream()
          .sorted(Comparator.comparing(SimpleFunctionInfo::getCategory)
              .thenComparing(SimpleFunctionInfo::getName))
          .collect(Collectors.toList());
      String prevCategory = sortedFunctions.get(0).getCategory();
      for (SimpleFunctionInfo fn : sortedFunctions) {
        if (!fn.getCategory().equals(prevCategory)) {
          builder.withRow(EMPTY_ROW);
        }
        builder.withRow(fn.getName(), fn.getCategory());
        prevCategory = fn.getCategory();
      }
    }
    builder.withFooterLine(
        "For detailed information about a function, run: DESCRIBE FUNCTION <Function Name>;");
    return builder.build();
  }
}
