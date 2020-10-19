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

package io.confluent.ksql.util;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.rest.entity.StreamedRow.DataRow;
import io.confluent.ksql.rest.entity.StreamedRow.Header;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.SimpleColumn;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Helper for converting a {@link DataRow} containing a tombstone into an array of column values
 * matching the schema defined in the supplied {@code header}.
 *
 * <p>Any key columns will be populated, other columns will contain {@code <TOMBSTONE>}.
 */
@Immutable
public final class TombstoneConverter {

  private final ImmutableMap<Integer, Integer> keyIndexes;
  private final int numColumns;

  public TombstoneConverter(final Header header) {
    this.keyIndexes = buildKeyIdx(header);
    this.numColumns = header.getColumnsSchema().value().size();
  }

  public List<?> asColumns(final DataRow row) {
    if (!row.getTombstone().orElse(false)) {
      throw new IllegalArgumentException("Not a tombstone: " + row);
    }

    final List<?> key = row.getKey()
        .orElseThrow(() -> new IllegalArgumentException("Not a table row: " + row));

    final List<Object> values = new ArrayList<>(numColumns);

    for (int columnIdx = 0; columnIdx < numColumns; columnIdx++) {
      final Integer keyIdx = keyIndexes.get(columnIdx);
      if (keyIdx == null) {
        values.add("<TOMBSTONE>");
      } else {
        values.add(key.get(keyIdx));
      }
    }

    return values;
  }

  private static ImmutableMap<Integer, Integer> buildKeyIdx(final Header header) {
    final List<ColumnName> keyColumns = header.getKeySchema()
        .orElseThrow(() -> new IllegalArgumentException("not a table: " + header))
        .stream()
        .map(SimpleColumn::name)
        .collect(Collectors.toList());

    final List<Column> columns = header.getColumnsSchema().value();

    final Map<ColumnName, Integer> columnIndexes = new HashMap<>(columns.size());
    for (int columnIndex = 0; columnIndex < columns.size(); columnIndex++) {
      final Column column = columns.get(columnIndex);
      columnIndexes.put(column.name(), columnIndex);
    }

    final Builder<Integer, Integer> builder = ImmutableMap.builder();
    for (int keyIndex = 0; keyIndex < keyColumns.size(); keyIndex++) {
      final Integer columnIndex = columnIndexes.get(keyColumns.get(keyIndex));
      if (columnIndex == null) {
        continue; // Not in projection
      }

      builder.put(columnIndex, keyIndex);
    }

    return builder.build();
  }
}
