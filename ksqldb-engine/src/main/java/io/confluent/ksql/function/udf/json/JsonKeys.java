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
    name = "JSON_KEYS",
    category = FunctionCategory.JSON,
    description = "Given a string, parses it as a JSON object and returns a ksqlDB array of "
        + "strings representing the top-level keys. Returns NULL if the string can't be "
        + "interpreted as a JSON object, for example, when the string is NULL or it does not "
        + "contain valid JSON, or the JSON value is not an object.",
    author = KsqlConstants.CONFLUENT_AUTHOR
)
public class JsonKeys {

  @Udf
  public List<String> keys(@UdfParameter final String jsonObj) {
    if (jsonObj == null) {
      return null;
    }

    final JsonNode node = UdfJsonMapper.parseJson(jsonObj);
    if (node.isMissingNode() || !node.isObject()) {
      return null;
    }

    final List<String> ret = new ArrayList<>();
    node.fieldNames().forEachRemaining(ret::add);
    return ret;
  }
}
