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

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.SerdeOption;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class PageViewDataProvider extends TestDataProvider {
  private static final String namePrefix =
      "PAGEVIEW";

  private static final String ksqlSchemaString = "(VIEWTIME bigint, USERID varchar, PAGEID varchar)";

  private static final String key = "VIEWTIME";

  private static final LogicalSchema schema = LogicalSchema.builder()
      .valueColumn(ColumnName.of("VIEWTIME"), SqlTypes.BIGINT)
      .valueColumn(ColumnName.of("USERID"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("PAGEID"), SqlTypes.STRING)
      .build();

  private static final Map<String, GenericRow> data = buildData();

  public PageViewDataProvider() {
    super(namePrefix, ksqlSchemaString, key, PhysicalSchema.from(schema, SerdeOption.none()), data);
  }

  private static Map<String, GenericRow> buildData() {
    final Map<String, GenericRow> dataMap = new HashMap<>();

    // Create page view records with:
    // key = page_id
    // value = (view time, user_id, page_id)
    dataMap.put("1", new GenericRow(Arrays.asList(1L, "USER_1", "PAGE_1")));
    dataMap.put("2", new GenericRow(Arrays.asList(2L, "USER_2", "PAGE_2")));
    dataMap.put("3", new GenericRow(Arrays.asList(3L, "USER_4", "PAGE_3")));
    dataMap.put("4", new GenericRow(Arrays.asList(4L, "USER_3", "PAGE_4")));
    dataMap.put("5", new GenericRow(Arrays.asList(5L, "USER_0", "PAGE_5")));

    // Duplicate page views from different users.
    dataMap.put("6", new GenericRow(Arrays.asList(6L, "USER_2", "PAGE_5")));
    dataMap.put("7", new GenericRow(Arrays.asList(7L, "USER_3", "PAGE_5")));

    return dataMap;
  }

}