/*
 * Copyright 2018 Confluent Inc.
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
import com.google.common.collect.ImmutableList;
import io.confluent.ksql.function.FunctionCategory;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;
import io.confluent.ksql.util.json.JsonPathTokenizer;
import java.util.List;

@UdfDescription(
    name = "extractjsonfield",
    category = FunctionCategory.JSON,
    description = "Given a STRING that contains JSON data, extract the value at the specified "
        + " JSONPath or NULL if the specified path does not exist.")
public class JsonExtractString {

  private String latestPath = null;
  private List<String> latestTokens = null;

  @Udf
  public String extract(
      @UdfParameter(description = "The input JSON string") final String input,
      @UdfParameter(description = "The JSONPath to extract") final String path) {

    if (input == null || path == null) {
      return null;
    }


    if (latestPath == null || !latestPath.equals(path)) {
      final JsonPathTokenizer tokenizer = new JsonPathTokenizer(path);
      latestTokens = ImmutableList.copyOf(tokenizer);
      latestPath = path;
    }

    JsonNode currentNode = UdfJsonMapper.parseJson(input);
    for (final String token : latestTokens) {
      if (currentNode instanceof ArrayNode) {
        try {
          final int index = Integer.parseInt(token);
          currentNode = currentNode.get(index);
        } catch (final NumberFormatException e) {
          return null;
        }
      } else {
        currentNode = currentNode.get(token);
      }

      if (currentNode == null) {
        return null;
      }
    }

    if (currentNode.isTextual()) {
      return currentNode.asText();
    } else {
      return currentNode.toString();
    }
  }
}
