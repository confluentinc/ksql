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

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.execution.util.StructKeyUtil.KeyBuilder;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import java.util.Collections;
import java.util.List;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.junit.Before;
import org.junit.Test;

public class StructKeyUtilTest {

  private static final LogicalSchema LOGICAL_SCHEMA = LogicalSchema.builder()
      .keyColumn(ColumnName.of("Bob"), SqlTypes.INTEGER)
      .valueColumn(ColumnName.of("DOES_NOT_MATTER"), SqlTypes.STRING)
      .build();

  private static final Schema STRUCT_SCHEMA = SchemaBuilder.struct()
      .field("ID", Schema.OPTIONAL_INT32_SCHEMA)
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
        .valueColumn(ColumnName.of("V0"), SqlTypes.STRING)
        .build());
  }

  @Test
  public void shouldBuildCorrectSchema() {
    // When:
    final Struct result = builder.build(1, 0);

    // Then:
    assertThat(result.schema(), is(SchemaBuilder.struct()
        .field("Bob", Schema.OPTIONAL_INT32_SCHEMA)
        .build()));
  }

  @Test
  public void shouldHandleValue() {
    // When:
    final Struct result = builder.build(1, 0);

    // Then:
    assertThat(result.getInt32("Bob"), is(1));
  }

  @Test
  public void shouldHandleNulls() {
    // When:
    final Struct result = builder.build(null, 0);

    // Then:
    assertThat(result.getInt32("Bob"), is(nullValue()));
  }

  @Test
  public void shouldConvertNonWindowedKeyToList() {
    // Given:
    final Struct key = new Struct(STRUCT_SCHEMA)
        .put("ID", 10);

    // When:
    final List<?> result = StructKeyUtil.asList(key);

    // Then:
    assertThat(result, is(ImmutableList.of(10)));
  }

  @Test
  public void shouldConvertNullKeyToList() {
    // Given:
    final Struct key = new Struct(STRUCT_SCHEMA)
        .put("ID", null);

    // When:
    final List<?> result = StructKeyUtil.asList(key);

    // Then:
    assertThat(result, is(Collections.singletonList((null))));
  }

  @Test
  public void shouldConvertWindowedKeyToList() {
    // Given:
    final Windowed<Struct> key = new Windowed<>(
        new Struct(STRUCT_SCHEMA).put("ID", 10),
        new TimeWindow(1000, 2000)
    );

    // When:
    final List<?> result = StructKeyUtil.asList(key);

    // Then:
    assertThat(result, is(ImmutableList.of(10, 1000L, 2000L)));
  }
}