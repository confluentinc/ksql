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

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.Column;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public final class TabularRow {

  private static final String CLIPPED = "...";
  private static final int MIN_CELL_WIDTH = 5;

  private final int cellWidth;
  private final List<String> columns;
  private final boolean isHeader;
  private final boolean shouldWrap;

  public static TabularRow createHeader(
      final int width,
      final List<Column> columns,
      final boolean shouldWrap,
      final int configuredCellWidth
  ) {
    final List<String> headings = columns.stream()
        .map(Column::name)
        .map(ColumnName::text)
        .collect(Collectors.toList());

    return new TabularRow(
        width,
        headings,
        true,
        shouldWrap,
        configuredCellWidth
    );
  }

  public static TabularRow createRow(
      final int width,
      final GenericRow value,
      final boolean shouldWrap,
      final int configuredCellWidth
  ) {
    return new TabularRow(
        width,
        value.values().stream().map(Objects::toString).collect(Collectors.toList()),
        false,
        shouldWrap,
        configuredCellWidth
    );
  }

  private TabularRow(
      final int width,
      final List<String> columns,
      final boolean isHeader,
      final boolean shouldWrap,
      final int configuredCellWidth
  ) {
    this.columns = ImmutableList.copyOf(Objects.requireNonNull(columns, "columns"));
    this.isHeader = isHeader;
    this.shouldWrap = isHeader || shouldWrap;

    if (configuredCellWidth > 0) {
      this.cellWidth = configuredCellWidth;
    } else if (!columns.isEmpty()) {
      this.cellWidth = Math.max(width / columns.size() - 2, MIN_CELL_WIDTH);
    } else {
      cellWidth = MIN_CELL_WIDTH;
    }
  }

  @Override
  public String toString() {
    if (columns.isEmpty()) {
      return "";
    }

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

    formatRow(builder, buffered, shouldWrap ? maxSplit : 1);

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
        final String colValue = columns.get(col).get(row);
        if (shouldClip(columns.get(col), numRows)) {
          builder.append(colValue, 0, colValue.length() - CLIPPED.length())
              .append(CLIPPED)
              .append('|');
        } else {
          builder.append(colValue)
              .append('|');
        }
      }
      if (row != numRows - 1) {
        builder.append('\n');
      }
    }
  }

  private static boolean shouldClip(final List<String> parts, final int rowsToPrint) {
    // clip if there are more than one line and any of the remaining lines are non-empty
    return parts.size() > rowsToPrint
        && !parts
        .subList(rowsToPrint, parts.size())
        .stream()
        .map(String::trim)
        .allMatch(String::isEmpty);
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
    final String format = "%-" + width + "s";
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
