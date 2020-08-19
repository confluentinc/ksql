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

import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.Multimap;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.SerdeOptions;

public class UserDataProvider extends TestDataProvider<String> {

  private static final LogicalSchema LOGICAL_SCHEMA = LogicalSchema.builder()
      .keyColumn(ColumnName.of("USERID"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("REGISTERTIME"), SqlTypes.BIGINT)
      .valueColumn(ColumnName.of("GENDER"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("REGIONID"), SqlTypes.STRING)
      .build();

  private static final PhysicalSchema PHYSICAL_SCHEMA = PhysicalSchema
      .from(LOGICAL_SCHEMA, SerdeOptions.of());

  private static final Multimap<String, GenericRow> ROWS = ImmutableListMultimap
      .<String, GenericRow>builder()
      .put("USER_0", genericRow(0L, "FEMALE", "REGION_0"))
      .put("USER_1", genericRow(1L, "MALE", "REGION_1"))
      .put("USER_2", genericRow(2L, "FEMALE", "REGION_1"))
      .put("USER_3", genericRow(3L, "MALE", "REGION_0"))
      .put("USER_4", genericRow(4L, "MALE", "REGION_4"))
      .build();

  public UserDataProvider() {
    super("USER", PHYSICAL_SCHEMA, ROWS);
  }
}