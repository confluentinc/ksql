/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql.api.client.util;

import io.confluent.ksql.api.client.ColumnType;
import io.confluent.ksql.api.client.impl.ColumnTypeImpl;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public final class RowUtil {

  private RowUtil() {
  }

  // Given list of values, return map of value to index in list. Returned indices are 1-indexed.
  public static Map<String, Integer> valueToIndexMap(final List<String> values) {
    final Map<String, Integer> valueToIndex = new HashMap<>();
    for (int i = 0; i < values.size(); i++) {
      valueToIndex.put(values.get(i), i + 1);
    }
    return valueToIndex;
  }

  public static List<ColumnType> columnTypesFromStrings(final List<String> columnTypes) {
    return columnTypes.stream().map(RowUtil::columnTypeFromString).collect(Collectors.toList());
  }

  private static ColumnType columnTypeFromString(final String columnType) {
    final int bracketInd = columnType.indexOf('<');
    final int parenInd = columnType.indexOf('(');

    final String primaryType = columnType.substring(0, Math.min(
        bracketInd == -1 ? columnType.length() : bracketInd,
        parenInd == -1 ? columnType.length() : parenInd
    ));
    return new ColumnTypeImpl(primaryType);
  }
}