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

package io.confluent.ksql.execution.util;

import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertThat;

import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import java.util.Set;
import org.junit.Test;

public class SinkSchemaUtilTest {
  @Test
  public void shouldComputeIndexesToRemoveImplicitsAndRowKey() {
    // Given:
    LogicalSchema schema = LogicalSchema.builder()
        .valueColumn(ColumnName.of("field1"), SqlTypes.STRING)
        .valueColumn(ColumnName.of("field2"), SqlTypes.STRING)
        .valueColumn(ColumnName.of("field3"), SqlTypes.STRING)
        .valueColumn(ColumnName.of("timestamp"), SqlTypes.BIGINT)
        .valueColumn(ColumnName.of("key"), SqlTypes.STRING)
        .build()
        .withMetaAndKeyColsInValue();

    // When:
    Set<Integer> indices = SinkSchemaUtil.implicitAndKeyColumnIndexesInValueSchema(schema);

    // Then:
    assertThat(indices, contains(0, 1));
  }

  @Test
  public void shouldComputeIndexesToRemoveImplicitsAndRowKeyRegardlessOfLocation() {
    // Given:
    LogicalSchema schema = LogicalSchema.builder()
        .valueColumn(ColumnName.of("field1"), SqlTypes.STRING)
        .valueColumn(ColumnName.of("field2"), SqlTypes.STRING)
        .valueColumn(ColumnName.of("ROWKEY"), SqlTypes.STRING)
        .valueColumn(ColumnName.of("field3"), SqlTypes.STRING)
        .valueColumn(ColumnName.of("timestamp"), SqlTypes.BIGINT)
        .valueColumn(ColumnName.of("ROWTIME"), SqlTypes.BIGINT)
        .valueColumn(ColumnName.of("key"), SqlTypes.STRING)
        .build();

    // When:
    Set<Integer> indices = SinkSchemaUtil.implicitAndKeyColumnIndexesInValueSchema(schema);

    // Then:
    assertThat(indices, contains(2, 5));
  }
}