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

public class ItemDataProvider extends TestDataProvider {

  private static final String namePrefix =
      "ITEM";

  private static final String ksqlSchemaString =
      "(ID varchar, DESCRIPTION varchar)";

  private static final String key = "ID";

  private static final Schema schema = SchemaBuilder.struct()
      .field("ID", SchemaBuilder.STRING_SCHEMA)
      .field("DESCRIPTION", SchemaBuilder.STRING_SCHEMA).build();

  private static final Map<String, GenericRow> data = new ItemDataProvider().buildData();

  public ItemDataProvider() {
    super(namePrefix, ksqlSchemaString, key, schema, data);
  }

  private Map<String, GenericRow> buildData() {

    Map<String, GenericRow> dataMap = new HashMap<>();
    dataMap.put("ITEM_1", new GenericRow(Arrays.asList("ITEM_1",  "home cinema")));
    dataMap.put("ITEM_2", new GenericRow(Arrays.asList("ITEM_2",  "clock radio")));
    dataMap.put("ITEM_3", new GenericRow(Arrays.asList("ITEM_3",  "road bike")));
    dataMap.put("ITEM_4", new GenericRow(Arrays.asList("ITEM_4",  "mountain bike")));
    dataMap.put("ITEM_5", new GenericRow(Arrays.asList("ITEM_5",  "snowboard")));
    dataMap.put("ITEM_6", new GenericRow(Arrays.asList("ITEM_6",  "iphone 10")));
    dataMap.put("ITEM_7", new GenericRow(Arrays.asList("ITEM_7",  "gopro")));
    dataMap.put("ITEM_8", new GenericRow(Arrays.asList("ITEM_8",  "cat")));

    return dataMap;
  }

}
