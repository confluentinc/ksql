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

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.GenericRow;
import java.util.Arrays;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

public class PageViewDataProvider extends TestDataProvider {
  private static final String namePrefix =
      "PAGEVIEW";

  private static final String ksqlSchemaString = "(VIEWTIME bigint, USERID varchar, PAGEID varchar)";


  private static final String key = "PAGEID";

  private static final Schema schema = SchemaBuilder.struct()
      .field("VIEWTIME", SchemaBuilder.OPTIONAL_INT64_SCHEMA)
      .field("USERID", SchemaBuilder.OPTIONAL_STRING_SCHEMA)
      .field("PAGEID", SchemaBuilder.OPTIONAL_STRING_SCHEMA).build();

  private static final Map<String, GenericRow> data = buildData();

  public PageViewDataProvider() {
    super(namePrefix, ksqlSchemaString, key, schema, data);
  }

  private static Map<String, GenericRow> buildData() {
    // Create page view records with:
    // key = page_id
    // value = (view time, user_id, page_id)
    return ImmutableMap.<String, GenericRow>builder()
        .put("1", new GenericRow(Arrays.asList(1L, "USER_1", "PAGE_1")))
        .put("2", new GenericRow(Arrays.asList(2L, "USER_2", "PAGE_2")))
        .put("3", new GenericRow(Arrays.asList(3L, "USER_4", "PAGE_3")))
        .put("4", new GenericRow(Arrays.asList(4L, "USER_3", "PAGE_4")))
        .put("5", new GenericRow(Arrays.asList(5L, "USER_0", "PAGE_5")))
        // Duplicate page views from different users.
        .put("6", new GenericRow(Arrays.asList(6L, "USER_2", "PAGE_5")))
        .put("7", new GenericRow(Arrays.asList(7L, "USER_3", "PAGE_5")))
        .build();
  }

}