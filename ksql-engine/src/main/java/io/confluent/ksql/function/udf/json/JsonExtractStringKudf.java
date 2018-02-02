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

package io.confluent.ksql.function.udf.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import io.confluent.ksql.function.KsqlFunctionException;
import io.confluent.ksql.function.udf.Kudf;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.json.JsonPathTokenizer;

import java.io.IOException;

public class JsonExtractStringKudf implements Kudf {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private String path = null;
  private ImmutableList<String> tokens = null;

  @Override
  public void init() {
  }

  @Override
  public Object evaluate(Object... args) {
    if (args.length != 2) {
      throw new KsqlFunctionException("getStringFromJson udf should have two input argument.");
    }
    String jsonString = args[0].toString();
    if (path == null) {
      path = args[1].toString();
      JsonPathTokenizer jsonPathTokenizer = new JsonPathTokenizer(path);
      tokens = ImmutableList.copyOf(jsonPathTokenizer);
    }
    JsonNode jsonNode;
    try {
      jsonNode = OBJECT_MAPPER.readTree(jsonString);
    } catch (IOException e) {
      throw new KsqlException("Invalid JSON format.", e);
    }
    JsonNode currentNode = jsonNode;
    for (String token: tokens) {
      if (currentNode == null) {
        return null;
      }
      try {
        int index = Integer.parseInt(token);
        currentNode = currentNode.get(index);
      } catch (NumberFormatException e) {
        currentNode = currentNode.get(token);
      }
    }
    if (currentNode == null) {
      return null;
    }
    if (currentNode.isTextual()) {
      return currentNode.asText();
    } else {
      return currentNode.toString();
    }
  }
}
