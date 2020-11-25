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

import static io.confluent.ksql.GenericKey.genericKey;
import static io.confluent.ksql.GenericRow.genericRow;

import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import io.confluent.ksql.GenericKey;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.SerdeFeatures;

/**
 * Provides a table where the changelog contains tombstones.
 */
public class TombstoneProvider extends TestDataProvider {

  private static final LogicalSchema LOGICAL_SCHEMA = LogicalSchema.builder()
      .keyColumn(ColumnName.of("ID"), SqlTypes.INTEGER)
      .valueColumn(ColumnName.of("VAL"), SqlTypes.STRING)
      .build();

  private static final PhysicalSchema PHYSICAL_SCHEMA = PhysicalSchema
      .from(LOGICAL_SCHEMA, SerdeFeatures.of(), SerdeFeatures.of());

  private static final Multimap<GenericKey, GenericRow> ROWS;

  static {
    final LinkedListMultimap<GenericKey, GenericRow> rows = LinkedListMultimap.create();
    rows.put(genericKey(1), genericRow("a"));
    rows.put(genericKey(1), null);
    rows.put(genericKey(2), genericRow("b"));
    rows.put(genericKey(3), genericRow("c"));
    rows.put(genericKey(2), null);
    ROWS = Multimaps.unmodifiableMultimap(rows);
  }

  public TombstoneProvider() {
    super("TOMBSTONE", PHYSICAL_SCHEMA, ROWS);
  }
}