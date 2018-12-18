/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
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
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

public class UserDataProvider extends TestDataProvider {
  private static final String namePrefix = "USER";

  private static final String ksqlSchemaString = "(REGISTERTIME bigint, GENDER varchar, REGIONID varchar, USERID varchar)";

  private static final String key = "USERID";

  private static final Schema schema = SchemaBuilder.struct()
      .field("REGISTERTIME", SchemaBuilder.OPTIONAL_INT64_SCHEMA)
      .field("GENDER", SchemaBuilder.OPTIONAL_STRING_SCHEMA)
      .field("REGIONID", SchemaBuilder.OPTIONAL_STRING_SCHEMA)
      .field("USERID", SchemaBuilder.OPTIONAL_STRING_SCHEMA).build();

  private static final Map<String, GenericRow> data = buildData();

  public UserDataProvider() {
    super(namePrefix, ksqlSchemaString, key, schema, data);
  }

  private static Map<String, GenericRow> buildData() {
    final Map<String, GenericRow> dataMap = new HashMap<>();
    // create a records with:
    // key == user_id
    // value = (creation_time, gender, region, user_id)
    dataMap.put("USER_0", new GenericRow(Arrays.asList(0L, "FEMALE", "REGION_0", "USER_0")));
    dataMap.put("USER_1", new GenericRow(Arrays.asList(1L, "MALE", "REGION_1", "USER_1")));
    dataMap.put("USER_2", new GenericRow(Arrays.asList(2L, "FEMALE", "REGION_1", "USER_2")));
    dataMap.put("USER_3", new GenericRow(Arrays.asList(3L, "MALE", "REGION_0", "USER_3")));
    dataMap.put("USER_4", new GenericRow(Arrays.asList(4L, "MALE", "REGION_4", "USER_4")));

    return dataMap;
  }


}