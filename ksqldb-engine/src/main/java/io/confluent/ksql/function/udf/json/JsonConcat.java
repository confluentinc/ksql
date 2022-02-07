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
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.confluent.ksql.function.FunctionCategory;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;
import io.confluent.ksql.util.KsqlConstants;
import java.util.ArrayList;
import java.util.List;

@UdfDescription(
    name = "JSON_CONCAT",
    category = FunctionCategory.JSON,
    description = "Given N strings, parse them as JSON values and return a string representing "
        + "their concatenation. Concatenation rules are identical to PostgreSQL's || operator:\n"
        + "* If all strings deserialize into JSON objects, return an object with a union of the "
        + "input keys. If there are duplicate objects, take values from the last object.\n"
        + "* If all strings deserialize into JSON arrays, return the result of array "
        + "concatenation.\n"
        + "* If at least one of the deserialized values is not an object, convert non-array"
        + " inputs to a single-element array and return the result of array concatenation.\n"
        + "* If at least one of the input strings is `NULL` or can't be deserialized as JSON, "
        + "return NULL.\n"
        + "Similar to the PostgreSQL's || operator, this function merges only top-level object "
        + "keys or arrays.",
    author = KsqlConstants.CONFLUENT_AUTHOR
)
public class JsonConcat {
  @Udf
  public String concat(@UdfParameter final String... jsonStrings) {
    if (jsonStrings == null) {
      return null;
    }
    final List<JsonNode> nodes = new ArrayList<>(jsonStrings.length);
    boolean allObjects = true;
    for (final String jsonString : jsonStrings) {
      if (jsonString == null) {
        return null;
      }

      final JsonNode node = UdfJsonMapper.parseJson(jsonString);
      if (node.isMissingNode()) {
        return null;
      }

      if (allObjects && !node.isObject()) {
        allObjects = false;
      }

      nodes.add(node);
    }

    JsonNode result = nodes.get(0);

    if (allObjects) {
      for (int i = 1; i < nodes.size(); i++) {
        result = concatObjects((ObjectNode) result, (ObjectNode) nodes.get(i));
      }
    } else {
      for (int i = 1; i < nodes.size(); i++) {
        result = concatArrays(toArrayNode(result), toArrayNode(nodes.get(i)));
      }
    }

    return UdfJsonMapper.writeValueAsJson(result);
  }

  private ObjectNode concatObjects(final ObjectNode acc, final ObjectNode node) {
    return acc.setAll(node);
  }

  private ArrayNode concatArrays(final ArrayNode acc, final ArrayNode node) {
    return acc.addAll(node);
  }

  private ArrayNode toArrayNode(final JsonNode node) {
    if (node.isArray()) {
      return (ArrayNode) node;
    }

    return UdfJsonMapper.INSTANCE.createArrayNode()
        .add(node);
  }
}
