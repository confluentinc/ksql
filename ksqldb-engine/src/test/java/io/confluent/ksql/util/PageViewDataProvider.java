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

public class PageViewDataProvider extends TestDataProvider<String> {

  private static final LogicalSchema LOGICAL_SCHEMA = LogicalSchema.builder()
      .keyColumn(ColumnName.of("PAGEID"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("USERID"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("VIEWTIME"), SqlTypes.BIGINT)
      .build();

  private static final PhysicalSchema PHYSICAL_SCHEMA = PhysicalSchema
      .from(LOGICAL_SCHEMA, SerdeOptions.of());

  private static final Multimap<String, GenericRow> ROWS = ImmutableListMultimap
      .<String, GenericRow>builder()
      .put("PAGE_1", genericRow("USER_1", 1L))
      .put("PAGE_2", genericRow("USER_2", 2L))
      .put("PAGE_3", genericRow("USER_4", 3L))
      .put("PAGE_4", genericRow("USER_3", 4L))
      .put("PAGE_5", genericRow("USER_0", 5L))
      // Duplicate page views from different users.
      .put("PAGE_5", genericRow("USER_2", 6L))
      .put("PAGE_5", genericRow("USER_3", 7L))
      .build();

  public PageViewDataProvider() {
    super("PAGEVIEW", PHYSICAL_SCHEMA, ROWS);
  }
}