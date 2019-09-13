/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.rest.entity;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.materialization.Window;
import io.confluent.ksql.rest.entity.QueryResultEntity.Row;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.SqlBaseType;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.stream.Collectors;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.Struct;

/**
 * Factory class for {@link QueryResultEntity}
 */
public final class QueryResultEntityFactory {

  private QueryResultEntityFactory() {
  }

  public static List<Row> createRows(
      final Struct key,
      final Map<Optional<Window>, GenericRow> rows,
      final LogicalSchema schema
  ) {
    final LinkedHashMap<String, ?> keyFields = toOrderedMap(key);

    return rows.entrySet().stream()
        .map(row -> new Row(window(row), keyFields, toOrderedMap(row.getValue(), schema.value())))
        .collect(Collectors.toList());
  }

  private static Optional<QueryResultEntity.Window> window(
      final Entry<Optional<Window>, GenericRow> row
  ) {
    return row.getKey()
        .map(w -> new QueryResultEntity.Window(
            w.start()
                .toEpochMilli(),
            w.end()
                .map(Instant::toEpochMilli)
                .map(OptionalLong::of)
                .orElse(OptionalLong.empty())
            ));
  }

  private static LinkedHashMap<String, ?> toOrderedMap(final Struct struct) {
    if (struct == null) {
      return null;
    }

    final LinkedHashMap<String, Object> orderedMap = new LinkedHashMap<>();

    for (int idx = 0; idx < struct.schema().fields().size(); idx++) {
      final org.apache.kafka.connect.data.Field field = struct.schema().fields().get(idx);
      final Object value = field.schema().type() == Type.STRUCT
          ? toOrderedMap((Struct) struct.get(field))
          : struct.get(field);

      orderedMap.put(field.name(), value);
    }

    return orderedMap;
  }

  private static LinkedHashMap<String, ?> toOrderedMap(
      final GenericRow row,
      final List<Column> schema
  ) {
    if (row.getColumns().size() != schema.size()) {
      throw new IllegalArgumentException("Column count mismatch."
          + " expected: " + schema.size()
          + ", got: " + row.getColumns().size()
      );
    }

    final LinkedHashMap<String, Object> orderedMap = new LinkedHashMap<>();

    for (int idx = 0; idx < row.getColumns().size(); idx++) {
      final Column column = schema.get(idx);
      final Object value = column.type().baseType() == SqlBaseType.STRUCT
          ? toOrderedMap((Struct) row.getColumns().get(idx))
          : row.getColumns().get(idx);

      orderedMap.put(column.fullName(), value);
    }

    return orderedMap;
  }
}
