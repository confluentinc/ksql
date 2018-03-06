/*
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

import com.google.common.collect.ImmutableList;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.node.ArrayNode;

import java.io.IOException;
import java.util.List;

import io.confluent.ksql.function.KsqlFunctionException;
import io.confluent.ksql.function.udf.Kudf;
import io.confluent.ksql.util.json.JsonPathTokenizer;

public class JsonExtractStringKudf implements Kudf {
  private static final ObjectReader OBJECT_READER = new ObjectMapper().reader();

  private List<String> tokens = null;

  @Override
  public Object evaluate(Object... args) {
    if (args.length != 2) {
      throw new KsqlFunctionException("getStringFromJson udf should have two input arguments:"
                                      + " JSON document and JSON path.");
    }

    ensureInitialized(args);

    JsonNode currentNode = parseJsonDoc(args[0]);
    for (String token : tokens) {
      if (currentNode instanceof ArrayNode) {
        try {
          final int index = Integer.parseInt(token);
          currentNode = currentNode.get(index);
        } catch (NumberFormatException e) {
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

  private void ensureInitialized(final Object[] args) {
    if (tokens != null) {
      return;
    }

    final String path = args[1].toString();
    final JsonPathTokenizer tokenizer = new JsonPathTokenizer(path);
    tokens = ImmutableList.copyOf(tokenizer);
  }

  private static JsonNode parseJsonDoc(final Object arg) {
    final String jsonString = arg.toString();
    try {
      return OBJECT_READER.readTree(jsonString);
    } catch (IOException e) {
      throw new KsqlFunctionException("Invalid JSON format:" + jsonString, e);
    }
  }
}
