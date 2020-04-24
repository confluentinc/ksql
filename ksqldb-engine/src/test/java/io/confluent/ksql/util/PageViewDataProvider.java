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

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.SerdeOption;
import java.util.Map;

public class PageViewDataProvider extends TestDataProvider<Long> {

  private static final LogicalSchema LOGICAL_SCHEMA = LogicalSchema.builder()
      .keyColumn(SchemaUtil.ROWKEY_NAME, SqlTypes.BIGINT)
      .valueColumn(ColumnName.of("VIEWTIME"), SqlTypes.BIGINT)
      .valueColumn(ColumnName.of("USERID"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("PAGEID"), SqlTypes.STRING)
      .build();

  private static final PhysicalSchema PHYSICAL_SCHEMA = PhysicalSchema
      .from(LOGICAL_SCHEMA, SerdeOption.none());

  private static final Map<Long, GenericRow> ROWS = ImmutableMap.<Long, GenericRow>builder()
      .put(1L, genericRow(1L, "USER_1", "PAGE_1"))
      .put(2L, genericRow(2L, "USER_2", "PAGE_2"))
      .put(3L, genericRow(3L, "USER_4", "PAGE_3"))
      .put(4L, genericRow(4L, "USER_3", "PAGE_4"))
      .put(5L, genericRow(5L, "USER_0", "PAGE_5"))
      // Duplicate page views from different users.
      .put(6L, genericRow(6L, "USER_2", "PAGE_5"))
      .put(7L, genericRow(7L, "USER_3", "PAGE_5"))
      .build();

  public PageViewDataProvider() {
    super("PAGEVIEW", "VIEWTIME", PHYSICAL_SCHEMA, ROWS);
  }
}