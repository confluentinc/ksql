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

@UdfDescription(
    name = "JSON_CONCAT",
    category = FunctionCategory.JSON,
    description = "Given 2 strings, parse them as JSON values and return a string representing "
        + "their concatenation. Concatenation rules are identical to PostgreSQL's || operator:"
        + "* If both strings deserialize into JSON objects, then return an object with a union of "
        + "the input key, taking values from the second object in the case of duplicates.\n"
        + "* If both strings deserialize into JSON arrays, then return the result of array "
        + "concatenation.\n"
        + "* If at least one of the deserialized values is not an object, then convert non-array "
        + "inputs to a single-element array and return the result of array concatenation.\n"
        + "* If at least one of the input strings is `NULL` or can't be deserialized as JSON, "
        + "then return `NULL`.\n\n"
        + "Akin to PostgreSQL's `||` operator, this function merges only top-level object keys "
        + "or arrays.",
    author = KsqlConstants.CONFLUENT_AUTHOR
)
public class JsonConcat {
  @Udf
  public String concat(
      @UdfParameter final String jsonString1,
      @UdfParameter final String jsonString2) {
    if (jsonString1 == null || jsonString2 == null) {
      return null;
    }
    final JsonNode node1 = UdfJsonMapper.parseJson(jsonString1);
    if (node1.isMissingNode()) {
      return null;
    }

    final JsonNode node2 = UdfJsonMapper.parseJson(jsonString2);
    if (node2.isMissingNode()) {
      return null;
    }

    if (node1.isObject() && node2.isObject()) {
      return concatObjects((ObjectNode) node1, (ObjectNode) node2);
    } else if (node1.isArray() && node2.isArray()) {
      return concatArrays((ArrayNode) node1, (ArrayNode) node2);
    } else {
      return concatArrays(toArrayNode(node1), toArrayNode(node2));
    }
  }

  private String concatObjects(final ObjectNode node1, final ObjectNode node2) {
    // it's okay to mutate node1 as the top method returns immediately without touching node1 again.
    return UdfJsonMapper.writeValueAsJson(node1.setAll(node2));
  }

  private String concatArrays(final ArrayNode node1, final ArrayNode node2) {
    // it's okay to mutate node1 as the top method returns immediately without touching node1 again.
    return UdfJsonMapper.writeValueAsJson(node1.addAll(node2));
  }

  private ArrayNode toArrayNode(final JsonNode node) {
    if (node.isArray()) {
      return (ArrayNode) node;
    }

    return UdfJsonMapper.INSTANCE.createArrayNode()
        .add(node);
  }
}
