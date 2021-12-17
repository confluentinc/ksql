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

package io.confluent.ksql.rest.server.resources.streaming;

import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.SimpleColumn;
import io.confluent.ksql.schema.ksql.SystemColumns;
import io.confluent.ksql.util.KeyValue;
import io.confluent.ksql.util.PushQueryMetadata.ResultType;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Helper to create {@link GenericRow GenericRows} that represent tombstones.
 *
 * <p>Such rows have any key columns in the projection populated, other columns will be {@code
 * null}.
 */
@Immutable
public final class TombstoneFactory {

  private final ImmutableMap<Integer, Integer> keyIndexes;
  private final int numColumns;

  public static TombstoneFactory create(final LogicalSchema schema, final ResultType resultType) {
    return new TombstoneFactory(buildKeyIdx(schema, resultType), schema.value().size());
  }

  private TombstoneFactory(final ImmutableMap<Integer, Integer> keyIndexes, final int numColumns) {
    this.keyIndexes = requireNonNull(keyIndexes, "keyIndexes");
    this.numColumns = numColumns;

    if (numColumns < 0) {
      throw new IllegalArgumentException("numColumns: " + numColumns + " < 0");
    }
  }

  public GenericRow createRow(final KeyValue<List<?>, GenericRow> row) {
    if (row.value() != null) {
      throw new IllegalArgumentException("Not a tombstone: " + row);
    }

    final List<?> key = row.key();
    if (key.size() < keyIndexes.size()) {
      throw new IllegalArgumentException("Not enough key columns. "
          + "expected at least" + keyIndexes.size() + ", got: " + key);
    }

    final GenericRow values = new GenericRow(numColumns);

    for (int columnIdx = 0; columnIdx < numColumns; columnIdx++) {
      final Integer keyIdx = keyIndexes.get(columnIdx);
      if (keyIdx == null) {
        values.append(null);
      } else {
        values.append(key.get(keyIdx));
      }
    }

    return values;
  }

  private static ImmutableMap<Integer, Integer> buildKeyIdx(
      final LogicalSchema schema, final ResultType resultType
  ) {
    final List<ColumnName> keyColumns = keyColumnNames(schema, resultType);

    final List<Column> projection = schema.value();

    final Map<ColumnName, Integer> columnIndexes = new HashMap<>(projection.size());
    for (int columnIndex = 0; columnIndex < projection.size(); columnIndex++) {
      final Column column = projection.get(columnIndex);
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

  private static List<ColumnName> keyColumnNames(
      final LogicalSchema schema,
      final ResultType resultType
  ) {
    final List<ColumnName> keyNames = schema.key().stream()
        .map(SimpleColumn::name)
        .collect(Collectors.toList());

    if (resultType == ResultType.WINDOWED_TABLE) {
      keyNames.addAll(SystemColumns.windowBoundsColumnNames());
    }

    return keyNames;
  }
}
