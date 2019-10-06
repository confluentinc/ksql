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
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.common.collect.ImmutableList;
import io.confluent.ksql.function.KsqlFunctionException;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;
import io.confluent.ksql.json.JsonMapper;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.json.JsonPathTokenizer;
import java.io.IOException;
import java.util.List;

@UdfDescription(name = "EXTRACTJSONFIELD", description = JsonExtractString.DESCRIPTION)
public class JsonExtractString {
  private static final ObjectReader OBJECT_READER = JsonMapper.INSTANCE.mapper.reader();
  static final String DESCRIPTION = "Extracts a field from json";

  @Udf
  public String extractJson(@UdfParameter final String json, @UdfParameter final String field) {
    if (json == null) {
      return null;
    }
    if (field == null) {
      throw new KsqlException("Path can not be null");
    }

    final JsonPathTokenizer tokenizer = new JsonPathTokenizer(field);
    final List<String> tokens = ImmutableList.copyOf(tokenizer);

    JsonNode currentNode = parseJsonDoc(json);
    for (final String token : tokens) {
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

  private static JsonNode parseJsonDoc(final Object arg) {
    final String jsonString = arg.toString();
    try {
      return OBJECT_READER.readTree(jsonString);
    } catch (final IOException e) {
      throw new KsqlFunctionException("Invalid JSON format:" + jsonString, e);
    }
  }
}
