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

package io.confluent.ksql.execution.plan;

import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.GenericKey;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import java.util.Objects;
import org.apache.kafka.streams.kstream.KGroupedTable;

@Immutable
public final class KGroupedTableHolder {

  private final KGroupedTable<GenericKey, GenericRow> groupedTable;
  private final LogicalSchema schema;

  private KGroupedTableHolder(
      final KGroupedTable<GenericKey, GenericRow> groupedTable,
      final LogicalSchema schema) {
    this.groupedTable = Objects.requireNonNull(groupedTable, "groupedTable");
    this.schema = Objects.requireNonNull(schema, "schema");
  }

  public static KGroupedTableHolder of(
      final KGroupedTable<GenericKey, GenericRow> groupedTable,
      final LogicalSchema schema
  ) {
    return new KGroupedTableHolder(groupedTable, schema);
  }

  public LogicalSchema getSchema() {
    return schema;
  }

  public KGroupedTable<GenericKey, GenericRow> getGroupedTable() {
    return groupedTable;
  }
}