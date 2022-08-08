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
import io.confluent.ksql.function.FunctionCategory;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;
import io.confluent.ksql.util.KsqlConstants;
import java.util.ArrayList;
import java.util.List;

@UdfDescription(
    name = "JSON_ITEMS",
    category = FunctionCategory.JSON,
    description = "Given a string with JSON array, converts it to an array of JSON strings and "
        + "returns an array of String. Returns `NULL` if input is `NULL` or each array item can't "
        + "be interpreted as a JSON object, i.e. it is `NULL` or it does not contain valid JSON, "
        + "or the JSON value is not an object.",
    author = KsqlConstants.CONFLUENT_AUTHOR
)
public class JsonItems {

  @Udf
  public List<String> items(@UdfParameter final String jsonItems) {
    if (jsonItems == null) {
      return null;
    }


    final List<JsonNode> objectList = UdfJsonMapper.readAsJsonArray(jsonItems);
    final List<String> res = new ArrayList<>();
    for (JsonNode jsonNode: objectList) {
      if (jsonNode.isMissingNode() || !jsonNode.isObject()) {
        return null;
      }
      res.add(jsonNode.toString());
    }
    return res;
  }
}
