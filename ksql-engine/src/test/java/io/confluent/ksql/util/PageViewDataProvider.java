/**
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/
package io.confluent.ksql.util;

import io.confluent.ksql.GenericRow;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class PageViewDataProvider extends TestDataProvider {
  private static final String namePrefix =
      "PAGEVIEW";

  private static final String ksqlSchemaString = "(VIEWTIME bigint, USERID varchar, PAGEID varchar)";


  private static final String key = "PAGEID";

  private static final Schema schema = SchemaBuilder.struct()
      .field("VIEWTIME", SchemaBuilder.INT64_SCHEMA)
      .field("USERID", SchemaBuilder.STRING_SCHEMA)
      .field("PAGEID", SchemaBuilder.STRING_SCHEMA).build();

  private static final Map<String, GenericRow> data = new PageViewDataProvider().buildData();

  public PageViewDataProvider() {
    super(namePrefix, ksqlSchemaString, key, schema, data);
  }

  private Map<String, GenericRow> buildData() {
    Map<String, GenericRow> dataMap = new HashMap<>();

    // Create page view records with:
    // key = page_id
    // value = (view time, user_id, page_id)
    dataMap.put("1", new GenericRow(Arrays.asList(1, "USER_1", "PAGE_1")));
    dataMap.put("2", new GenericRow(Arrays.asList(2, "USER_2", "PAGE_2")));
    dataMap.put("3", new GenericRow(Arrays.asList(3, "USER_4", "PAGE_3")));
    dataMap.put("4", new GenericRow(Arrays.asList(4, "USER_3", "PAGE_4")));
    dataMap.put("5", new GenericRow(Arrays.asList(5, "USER_0", "PAGE_5")));

    // Duplicate page views from different users.
    dataMap.put("6", new GenericRow(Arrays.asList(6, "USER_2", "PAGE_5")));
    dataMap.put("7", new GenericRow(Arrays.asList(7, "USER_3", "PAGE_5")));

    return dataMap;
  }

}
