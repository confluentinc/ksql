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

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.confluent.ksql.function.KsqlFunctionException;
import java.util.List;

/**
 * Shared Object mapper used by JSON processing UDFs
 */
final class UdfJsonMapper {

  private UdfJsonMapper() {}

  /**
   * It is thread-safe to share an instance of the configured ObjectMapper
   * (see https://fasterxml.github.io/jackson-databind/javadoc/2.12/com/fasterxml/jackson/databind/ObjectReader.html for more details).
   * The object is configured as part of static initialization, so it is published safely
   * as well.
   */
  public static final ObjectMapper INSTANCE;
  /**
   * Akin to the {@link UdfJsonMapper#INSTANCE}, the reader is fully thread-safe, so there is
   * no need to construct more than one instance. See https://fasterxml.github.io/jackson-databind/javadoc/2.12/com/fasterxml/jackson/databind/ObjectReader.html
   * for more details.
   */
  private static final ObjectReader OBJECT_READER;

  static {
    INSTANCE = new ObjectMapper()
        .disable(JsonGenerator.Feature.AUTO_CLOSE_TARGET)
        .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
        .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
        .enable(JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN)
        .setNodeFactory(JsonNodeFactory.withExactBigDecimals(true));
    OBJECT_READER = INSTANCE.reader();
  }

  /**
   * Parses string into a {@link JsonNode}; throws {@link KsqlFunctionException} on invalid JSON.
   *
   * @param jsonString the string to parse
   * @return a JSON node
   */
  public static JsonNode parseJson(final String jsonString) {
    try {
      return OBJECT_READER.readTree(jsonString);
    } catch (final JacksonException e) {
      throw new KsqlFunctionException("Invalid JSON format:" + jsonString, e);
    }
  }

  public static String writeValueAsJson(final Object obj) {
    try {
      return UdfJsonMapper.INSTANCE.writeValueAsString(obj);
    } catch (JsonProcessingException e) {
      throw new KsqlFunctionException("JSON serialization error: " + e.getMessage());
    }
  }

  public static List<JsonNode> readAsJsonArray(final String jsonString) {
    try {
      return UdfJsonMapper.INSTANCE.readValue(jsonString, new TypeReference<List<JsonNode>>() {});
    } catch (JsonProcessingException e) {
      throw new KsqlFunctionException("Invalid JSON format:" + jsonString, e);
    }
  }
}