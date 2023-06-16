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

@UdfDescription(
    name = "JSON_ARRAY_LENGTH",
    category = FunctionCategory.JSON,
    description = "Given a string, parses it as a JSON value and returns the length of the "
        + "top-level array. Returns NULL if the string can't be interpreted as a JSON array, "
        + "for example, when the string is `NULL` or it does not contain valid JSON, or the JSON "
        + "value is not an array.",
    author = KsqlConstants.CONFLUENT_AUTHOR
)
public class JsonArrayLength {

  @Udf
  public Integer length(@UdfParameter final String jsonArray) {
    if (jsonArray == null) {
      return null;
    }

    final JsonNode node = UdfJsonMapper.parseJson(jsonArray);
    if (node.isMissingNode() || !node.isArray()) {
      return null;
    }

    return node.size();
  }
}
