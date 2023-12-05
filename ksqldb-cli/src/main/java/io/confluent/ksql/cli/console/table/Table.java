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

package io.confluent.ksql.cli.console.table;

import static java.util.Objects.requireNonNull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.cli.console.Console;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.StringUtils;

public final class Table {

  private final ImmutableList<String> columnHeaders;
  private final ImmutableList<List<String>> rowValues;
  private final ImmutableList<String> header;
  private final ImmutableList<String> footer;

  private Table(
      final List<String> columnHeaders,
      final List<List<String>> rowValues,
      final List<String> header,
      final List<String> footer
  ) {
    this.columnHeaders = ImmutableList.copyOf(
        requireNonNull(columnHeaders, "columnHeaders")
    );
    this.rowValues = ImmutableList.copyOf(requireNonNull(rowValues, "rowValues"));
    this.header = ImmutableList.copyOf(requireNonNull(header, "header"));
    this.footer = ImmutableList.copyOf(requireNonNull(footer, "footer"));
  }

  @VisibleForTesting
  @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "columnHeaders is ImmutableList")
  public List<String> headers() {
    return columnHeaders;
  }

  @VisibleForTesting
  @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "rowValues is ImmutableList")
  public List<List<String>> rows() {
    return rowValues;
  }

  public static final class Builder {

    private final List<String> columnHeaders = new LinkedList<>();
    private final List<List<String>> rowValues = new LinkedList<>();
    private final List<String> header = new LinkedList<>();
    private final List<String> footer = new LinkedList<>();

    public Table build() {
      return new Table(columnHeaders, rowValues, header, footer);
    }

    public Builder withColumnHeaders(final List<String> columnHeaders) {
      this.columnHeaders.addAll(columnHeaders);
      return this;
    }

    public Builder withColumnHeaders(final String... columnHeaders) {
      this.columnHeaders.addAll(Arrays.asList(columnHeaders));
      return this;
    }

    public Builder withRows(final List<List<String>> rowValues) {
      this.rowValues.addAll(rowValues);
      return this;
    }

    public Builder withRows(final Stream<List<String>> rowValues) {
      rowValues.forEach(this.rowValues::add);
      return this;
    }

    public Builder withRow(final String... row) {
      this.rowValues.add(Arrays.asList(row));
      return this;
    }

    public Builder withRow(final List<String> row) {
      this.rowValues.add(row);
      return this;
    }

    public Builder withHeaderLine(final String headerLine) {
      this.header.add(headerLine);
      return this;
    }

    public Builder withFooterLine(final String footerLine) {
      this.footer.add(footerLine);
      return this;
    }
  }

  private int getMultiLineStringLength(final String multiLineString) {
    final String[] split = multiLineString.split(System.lineSeparator());
    return Arrays.stream(split)
        .mapToInt(String::length)
        .max()
        .orElse(0);
  }

  private int getColumnLength(final List<String> columnHeaders,
      final List<List<String>> rowValues,
      final int i) {
    return Math.max(
        columnHeaders.get(i).length(),
        rowValues
            .stream()
            .mapToInt(r -> getMultiLineStringLength(r.get(i)))
            .max()
            .orElse(0));
  }

  public void print(final Console console) {

    header.forEach(m -> console.writer().println(m));

    if (columnHeaders.size() > 0) {
      console.addResult(rowValues);

      final Integer[] columnLengths = new Integer[columnHeaders.size()];
      int separatorLength = -1;

      for (int i = 0; i < columnLengths.length; i++) {
        final int columnLength = getColumnLength(columnHeaders, rowValues, i);
        columnLengths[i] = columnLength;
        separatorLength += columnLength + 3;
      }

      final String rowFormatString = constructRowFormatString(columnLengths);

      console.writer().printf(rowFormatString, columnHeaders.toArray());

      final String separator =
          StringUtils.repeat('-', Math.min(console.getWidth(), separatorLength));
      console.writer().println(separator);
      for (final List<String> row : rowValues) {
        console.writer().printf(rowFormatString, row.toArray());
      }
      console.writer().println(separator);
    }

    footer.forEach(m -> console.writer().println(m));

    console.flush();
  }

  private static String constructRowFormatString(final Integer... lengths) {
    final List<String> columnFormatStrings = Arrays.stream(lengths)
        .map(Table::constructSingleColumnFormatString)
        .collect(Collectors.toList());
    return String.format(" %s %n", String.join(" | ", columnFormatStrings));
  }

  private static String constructSingleColumnFormatString(final Integer length) {
    return String.format("%%%ds", (-1 * length));
  }
}
