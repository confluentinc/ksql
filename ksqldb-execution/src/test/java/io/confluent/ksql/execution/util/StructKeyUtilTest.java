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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import io.confluent.ksql.execution.util.StructKeyUtil.KeyBuilder;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.Before;
import org.junit.Test;

public class StructKeyUtilTest {

  private static final LogicalSchema LOGICAL_SCHEMA = LogicalSchema.builder()
      .keyColumn(ColumnName.of("BOB"), SqlTypes.INTEGER)
      .valueColumn(ColumnName.of("DOES_NOT_MATTER"), SqlTypes.STRING)
      .build();
  private KeyBuilder builder;

  @Before
  public void setUp() {
    builder = StructKeyUtil.keyBuilder(LOGICAL_SCHEMA);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void shouldThrowOnMultipleKeyColumns() {
    // Only single key columns initially supported
    StructKeyUtil.keyBuilder(LogicalSchema.builder()
        .keyColumn(ColumnName.of("BOB"), SqlTypes.STRING)
        .keyColumn(ColumnName.of("JOHN"), SqlTypes.STRING)
        .build());
  }

  @Test
  public void shouldBuildCorrectSchema() {
    // When:
    final Struct result = builder.build(1);

    // Then:
    assertThat(result.schema(), is(SchemaBuilder.struct()
        .field("ROWKEY", Schema.OPTIONAL_INT32_SCHEMA)
        .build()));
  }

  @Test
  public void shouldHandleValue() {
    // When:
    final Struct result = builder.build(1);

    // Then:
    assertThat(result.getInt32("ROWKEY"), is(1));
  }

  @Test
  public void shouldHandleNulls() {
    // When:
    final Struct result = builder.build(null);

    // Then:
    assertThat(result.getInt32("ROWKEY"), is(nullValue()));
  }
}