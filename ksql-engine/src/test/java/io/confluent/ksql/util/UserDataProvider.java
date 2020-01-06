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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.SerdeOption;
import java.util.Map;

public class UserDataProvider extends TestDataProvider<String> {

  private static final LogicalSchema LOGICAL_SCHEMA = LogicalSchema.builder()
      .valueColumn(ColumnName.of("REGISTERTIME"), SqlTypes.BIGINT)
      .valueColumn(ColumnName.of("GENDER"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("REGIONID"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("USERID"), SqlTypes.STRING)
      .build();

  private static final PhysicalSchema PHYSICAL_SCHEMA = PhysicalSchema
      .from(LOGICAL_SCHEMA, SerdeOption.none());

  private static final Map<String, GenericRow> ROWS = ImmutableMap.<String, GenericRow>builder()
      .put("USER_0", new GenericRow(ImmutableList.of(0L, "FEMALE", "REGION_0", "USER_0")))
      .put("USER_1", new GenericRow(ImmutableList.of(1L, "MALE", "REGION_1", "USER_1")))
      .put("USER_2", new GenericRow(ImmutableList.of(2L, "FEMALE", "REGION_1", "USER_2")))
      .put("USER_3", new GenericRow(ImmutableList.of(3L, "MALE", "REGION_0", "USER_3")))
      .put("USER_4", new GenericRow(ImmutableList.of(4L, "MALE", "REGION_4", "USER_4")))
      .build();

  public UserDataProvider() {
    super("USER", "USERID", PHYSICAL_SCHEMA, ROWS);
  }
}