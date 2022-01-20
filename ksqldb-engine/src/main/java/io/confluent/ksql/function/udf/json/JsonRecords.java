/*
 * Copyright 2022 Confluent Inc.
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

package io.confluent.ksql.function.udf.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.annotations.VisibleForTesting;
import io.confluent.ksql.function.FunctionCategory;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;
import io.confluent.ksql.util.KsqlConstants;
import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

@UdfDescription(
    name = "JSON_RECORDS",
    category = FunctionCategory.JSON,
    description = "Given a string, parses it as a JSON object and returns a ksqlDB array of "
        + "structs containing 2 strings - json_key and json_value representing the top-level "
        + "keys and values. Returns NULL if the string can't be interpreted as a JSON object, "
        + "for example, when the string is NULL or it does not contain valid JSON, or the JSON "
        + "value is not an object.",
    author = KsqlConstants.CONFLUENT_AUTHOR
)
public class JsonRecords {
  static final String KEY_FIELD_NAME = "JSON_KEY";
  private static final String VALUE_FIELD_NAME = "JSON_VALUE";

  @VisibleForTesting
  static final Schema STRUCT_SCHEMA = SchemaBuilder.struct()
      .field(KEY_FIELD_NAME, Schema.OPTIONAL_STRING_SCHEMA)
      .field(VALUE_FIELD_NAME, Schema.OPTIONAL_STRING_SCHEMA)
      .optional()
      .build();

  @Udf(schema = "ARRAY<STRUCT<JSON_KEY STRING, JSON_VALUE STRING>>")
  public List<Struct> records(@UdfParameter final String jsonObj) {
    if (jsonObj == null) {
      return null;
    }

    final JsonNode node = UdfJsonMapper.parseJson(jsonObj);
    if (node.isMissingNode() || !node.isObject()) {
      return null;
    }

    final List<Struct> ret = new ArrayList<>();
    node.fieldNames().forEachRemaining(k -> {
      final Struct struct = new Struct(STRUCT_SCHEMA);
      struct.put(KEY_FIELD_NAME, k);
      struct.put(VALUE_FIELD_NAME, node.get(k).toString());
      ret.add(struct);
    });
    return ret;
  }
}
