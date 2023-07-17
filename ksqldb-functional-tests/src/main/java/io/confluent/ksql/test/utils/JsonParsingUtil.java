/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.test.utils;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import io.confluent.ksql.test.tools.exceptions.MissingFieldException;
import java.io.IOException;
import java.util.Optional;

public final class JsonParsingUtil {

  private JsonParsingUtil() {
  }

  public static <T> T getRequired(
      final String name,
      final JsonNode node,
      final JsonParser jp,
      final Class<T> type
  ) throws IOException {
    if (!node.has(name)) {
      throw new MissingFieldException(name);
    }

    return getNode(name, node, jp, type);
  }

  public static <T> Optional<T> getOptional(
      final String name,
      final JsonNode node,
      final JsonParser jp,
      final Class<T> type
  ) throws IOException {
    if (!node.has(name)) {
      return Optional.empty();
    }

    return Optional.ofNullable(getNode(name, node, jp, type));
  }

  public static <T> Optional<T> getOptional(
      final String name,
      final JsonNode node,
      final JsonParser jp,
      final TypeReference<T> type
  ) throws IOException {
    if (!node.has(name)) {
      return Optional.empty();
    }

    return Optional.ofNullable(getNode(name, node, jp, type));
  }

  public static <T> T getOrElse(
      final String name,
      final JsonNode node,
      final JsonParser jp,
      final Class<T> type,
      final T defaultValue
  ) throws IOException {
    return getOptionalOrElse(name, node, jp, type, defaultValue)
        .orElse(defaultValue);
  }

  /**
   * @return {@code empty()} if the node is not present, {@code defaultValue} of the node is
   *     present, but explicitly set to `null`, otherwise returns the value.
   */
  public static <T> Optional<T> getOptionalOrElse(
      final String name,
      final JsonNode node,
      final JsonParser jp,
      final Class<T> type,
      final T defaultValue
  ) throws IOException {
    if (!node.has(name)) {
      return Optional.empty();
    }

    final T t = getOptional(name, node, jp, type)
        .orElse(defaultValue);

    return Optional.of(t);
  }

  private static <T> T getNode(
      final String name,
      final JsonNode node,
      final JsonParser jp,
      final TypeReference<T> type
  ) throws IOException {
    return node
        .get(name)
        .traverse(jp.getCodec())
        .readValueAs(type);
  }

  private static <T> T getNode(
      final String name,
      final JsonNode node,
      final JsonParser jp,
      final Class<T> type
  ) throws IOException {
    return node
        .get(name)
        .traverse(jp.getCodec())
        .readValueAs(type);
  }
}
