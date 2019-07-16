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

  private static final int MIN_CELL_WIDTH = 5;

  private final int width;
  private final List<String> value;
  private final List<String> header;
  private final boolean isHeader;

  public static TabularRow createHeader(final int width, final List<FieldInfo> header) {
    return new TabularRow(
        width,
        header.stream().map(FieldInfo::getName).collect(Collectors.toList()),
        null);
  }

  public static TabularRow createRow(
      final int width,
      final List<FieldInfo> header,
      final GenericRow value
  ) {
    return new TabularRow(
        width,
        header.stream().map(FieldInfo::getName).collect(Collectors.toList()),
        value.getColumns().stream().map(Objects::toString).collect(Collectors.toList())
    );
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
    this.isHeader = value == null;
  }

  @Override
  public String toString() {
    final List<String> columns = isHeader ? header : value;

    if (columns.isEmpty()) {
      return "";
    }

    final int cellWidth = Math.max(width / columns.size() - 2, MIN_CELL_WIDTH);
    final StringBuilder builder = new StringBuilder();

    if (isHeader) {
      separatingLine(builder, cellWidth, columns.size());
      builder.append('\n');
    }

    // split each column into fix length chunks
    final List<List<String>> split = columns.stream()
        .map(col -> splitToFixed(col, cellWidth))
        .collect(Collectors.toList());

    // buffer each column vertically to have the max number of splits
    final int maxSplit = split.stream().mapToInt(List::size).max().orElse(0);
    final List<List<String>> buffered = split.stream()
        .map(s -> addUntil(s, createCell("", cellWidth), maxSplit))
        .collect(Collectors.toList());

    formatRow(builder, buffered, maxSplit);

    if (isHeader) {
      builder.append('\n');
      separatingLine(builder, cellWidth, columns.size());
    }

    return builder.toString();
  }

  @SuppressWarnings("ForLoopReplaceableByForEach") // clearer to read this way
  private static void formatRow(
      final StringBuilder builder,
      final List<List<String>> columns,
      final int numRows
  ) {
    for (int row = 0; row < numRows; row++) {
      builder.append('|');
      for (int col = 0; col < columns.size(); col++) {
        builder.append(columns.get(col).get(row));
      }
      if (row != numRows - 1) {
        builder.append('\n');
      }
    }
  }

  @SuppressWarnings("UnstableApiUsage")
  private static List<String> splitToFixed(final String value, final int width) {
    return Splitter.fixedLength(width)
        .splitToList(value)
        .stream()
        .map(line -> createCell(line, width))
        .collect(Collectors.toList());
  }

  private static void separatingLine(
      final StringBuilder builder,
      final int cellWidth,
      final int numColumns
  ) {
    builder.append("+");
    for (int i = 0; i < numColumns; i++) {
      builder.append(Strings.repeat("-", cellWidth));
      builder.append("+");
    }
  }

  private static String createCell(final String value, final int width) {
    final String format = "%-" + width + "s|";
    return String.format(format, value);
  }

  private static <T> List<T> addUntil(final List<T> source, final T value, final int desiredSize) {
    final List<T> copy = new ArrayList<>(source) ;
    while (copy.size() < desiredSize) {
      copy.add(value);
    }
    return copy;
  }
}
