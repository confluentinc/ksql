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

package io.confluent.ksql.api.client.util;

import io.confluent.ksql.api.client.ColumnType;
import io.confluent.ksql.api.client.impl.ColumnTypeImpl;
import java.util.HashMap;
import java.util.LinkedList;
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

  public static List<String> colNamesFromSchema(final String schema) {
    return splitAcrossOneLevelDeepComma(schema).stream()
            .map(RowUtil::removeBackTickAndKeyTrim)
            .map(RowUtil::splitAndGetFirst)
            .collect(Collectors.toList());
  }

  public static List<String> colTypesFromSchema(final String schema) {
    return splitAcrossOneLevelDeepComma(schema).stream()
            .map(RowUtil::removeBackTickAndKeyTrim)
            .map(RowUtil::splitAndGetSecond)
            .collect(Collectors.toList());
  }

  private static List<String> splitAcrossOneLevelDeepComma(final String stringWithCommas) {
    final List<String> listCols = new LinkedList<>();
    final StringBuilder sb = new StringBuilder();
    int nestRound = 0;
    int nestChevron = 0;
    for (int i = 0; i < stringWithCommas.length(); ++i) {
      final char ch = stringWithCommas.charAt(i);
      if (i == stringWithCommas.length() - 1) {
        sb.append(ch);
        listCols.add(sb.toString());
        break;
      }
      switch (ch) {
        case ',':
          if (nestRound == 0 && nestChevron == 0) {
            listCols.add(sb.toString());
            sb.setLength(0);
            continue;
          }
          break;
        case '(':
          ++nestRound;
          break;
        case ')':
          --nestRound;
          break;
        case '<':
          ++nestChevron;
          break;
        case '>':
          --nestChevron;
          break;
        default:
      }
      sb.append(ch);
    }
    return listCols;
  }

  private static String removeBackTickAndKeyTrim(final String dirtyString) {
    return dirtyString.replace(" KEY", "").replace("`", "").trim();
  }

  private static String splitAndGetFirst(final String stringToSplit) {
    return stringToSplit.split(" ", 2)[0];
  }

  private static String splitAndGetSecond(final String stringToSplit) {
    return stringToSplit.split(" ", 2)[1];
  }
}