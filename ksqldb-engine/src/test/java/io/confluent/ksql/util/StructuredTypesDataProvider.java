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
package io.confluent.ksql.util;

import static io.confluent.ksql.GenericRow.genericRow;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.SerdeOption;
import java.math.BigDecimal;
import java.util.Collections;

public class StructuredTypesDataProvider extends TestDataProvider<String> {

  private static final LogicalSchema LOGICAL_SCHEMA = LogicalSchema.builder()
      .keyColumn(ColumnName.of("STR"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("LONG"), SqlTypes.BIGINT)
      .valueColumn(ColumnName.of("DEC"), SqlTypes.decimal(4, 2))
      .valueColumn(ColumnName.of("ARRAY"), SqlTypes.array(SqlTypes.STRING))
      .valueColumn(ColumnName.of("MAP"), SqlTypes.map(SqlTypes.STRING))
      .build();

  private static final PhysicalSchema PHYSICAL_SCHEMA = PhysicalSchema
      .from(LOGICAL_SCHEMA, SerdeOption.none());

  private static final Multimap<String, GenericRow> ROWS = ImmutableListMultimap
      .<String, GenericRow>builder()
      .put("FOO", genericRow(1L, new BigDecimal("1.11"), Collections.singletonList("a"), Collections.singletonMap("k1", "v1")))
      .put("BAR", genericRow(2L, new BigDecimal("2.22"), Collections.emptyList(), Collections.emptyMap()))
      .put("BAZ", genericRow(3L, new BigDecimal("30.33"), Collections.singletonList("b"), Collections.emptyMap()))
      .put("BUZZ", genericRow(4L, new BigDecimal("40.44"), ImmutableList.of("c", "d"), Collections.emptyMap()))
      // Additional entries for repeated keys
      .put("BAZ", genericRow(5L, new BigDecimal("12"), ImmutableList.of("e"), ImmutableMap.of("k1", "v1", "k2", "v2")))
      .put("BUZZ", genericRow(6L, new BigDecimal("10.1"), ImmutableList.of("f", "g"), Collections.emptyMap()))
      .build();

  public StructuredTypesDataProvider() {
    super("STRUCTURED_TYPES", PHYSICAL_SCHEMA, ROWS);
  }
}