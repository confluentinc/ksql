/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.util;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.rest.entity.FieldInfo;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class TabularRow {

  private final int width;
  private final List<String> value;
  private final List<String> header;

  public TabularRow(
      final int width,
      final List<FieldInfo> header,
      final GenericRow value
  ) {
    this(
        width,
        header.stream().map(FieldInfo::getName).collect(Collectors.toList()),
        value == null
            ? null
            : value.getColumns().stream().map(Objects::toString).collect(Collectors.toList()));
  }

  @VisibleForTesting
  TabularRow(
      final int width,
      final List<String> header,
      final List<String> value
  ) {
    this.header = Objects.requireNonNull(header, "header");
    this.width = width;
    this.value = value;
  }

  @Override
  public String toString() {
    final List<String> columns = value == null ? header : value;

    final int cellWidth = width / columns.size() - 2;
    final String format = "%-" + cellWidth + "s|";

    // split each column into fix length chunks
    final List<List<String>> split = new ArrayList<>();
    for (String column : columns) {
      split.add(
          Splitter.fixedLength(cellWidth)
              .splitToList(column)
              .stream()
              .map(line -> String.format(format, line))
              .collect(Collectors.toList())
      );
    }

    // buffer each column vertically to have the max number of splits
    final int maxSplit = split.stream().map(List::size).max(Integer::compareTo).orElse(0);
    final List<List<String>> buffered = split.stream()
        .map(s -> addUntil(s, String.format(format, ""), maxSplit))
        .collect(Collectors.toList());

    // construct the actual table row by printing the first segment
    // of each column before moving onto the next segment of each
    // column
    final StringBuilder builder = new StringBuilder();
    for (int i = 0; i < maxSplit; i++) {
      builder.append('|');
      for (List<String> col : buffered) {
        builder.append(col.get(i));
      }
      builder.append('\n');
    }

    // add line at the end to separate one TabularRow from the next
    final String ruleChar = value == null ? "═" : "-";
    builder.append("+");
    for (int i = 0; i < columns.size(); i++) {
      builder.append(Strings.repeat(ruleChar, cellWidth));
      builder.append("+");
    }

    return builder.toString();
  }

  private static <T> List<T> addUntil(final List<T> source, final T value, final int desiredSize) {
    final List<T> copy = new ArrayList<>(source) ;
    while (copy.size() < desiredSize) {
      copy.add(value);
    }
    return copy;
  }
}
