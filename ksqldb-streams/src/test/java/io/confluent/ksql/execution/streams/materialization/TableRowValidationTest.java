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

package io.confluent.ksql.execution.streams.materialization;

import io.confluent.ksql.GenericKey;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import org.junit.Test;

public class TableRowValidationTest {

  private static final LogicalSchema SCHEMA = LogicalSchema.builder()
      .keyColumn(ColumnName.of("k0"), SqlTypes.STRING)
      .keyColumn(ColumnName.of("k1"), SqlTypes.INTEGER)
      .valueColumn(ColumnName.of("v0"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("v1"), SqlTypes.DOUBLE)
      .build();

  private static final GenericKey A_KEY = GenericKey.genericKey("key", 11);

  private static final GenericRow A_VALUE = GenericRow.genericRow("v0-v", 1.0d);

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowOnKeyFieldCountMismatch() {
    // Given:
    final GenericKey key = GenericKey.genericKey("key");

    // When:
    TableRowValidation.validate(SCHEMA, key, A_VALUE);
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowOnValueFieldCountMismatch() {
    // Given:
    final GenericRow valueWithLessFields = GenericRow.genericRow("v0-v");

    // When:
    TableRowValidation.validate(SCHEMA, A_KEY, valueWithLessFields);
  }

  @Test
  public void shouldNotThrowOnMatching() {
    TableRowValidation.validate(SCHEMA, A_KEY, A_VALUE);
  }
}