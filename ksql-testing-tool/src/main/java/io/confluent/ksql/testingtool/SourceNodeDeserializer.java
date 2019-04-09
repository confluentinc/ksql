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

package io.confluent.ksql.testingtool;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.node.NullNode;
import java.io.IOException;
import java.util.Optional;

public class SourceNodeDeserializer extends StdDeserializer<SourceNode> {

  public SourceNodeDeserializer() {
    super(SourceNode.class);
  }

  @Override
  public SourceNode deserialize(
      final JsonParser jp,
      final DeserializationContext ctxt
  ) throws IOException {

    final JsonNode node = jp.getCodec().readTree(jp);

    final String name = buildString("name", node, jp);
    final String type = buildString("type", node, jp);
    final Optional<FieldNode> keyField = buildKeyField(node, jp);

    return new SourceNode(name, type, keyField);
  }

  private static String buildString(
      final String name,
      final JsonNode node,
      final JsonParser jp
  ) throws IOException {
    return node.get(name).traverse(jp.getCodec()).readValueAs(String.class);
  }

  private static Optional<FieldNode> buildKeyField(
      final JsonNode node,
      final JsonParser jp
  ) throws IOException {
    if (!node.has("keyField")) {
      return Optional.empty();
    }

    final JsonNode keyField = node.get("keyField");
    if (keyField instanceof NullNode) {
      return Optional.of(FieldNode.NULL);
    }

    return Optional.of(keyField.traverse(jp.getCodec()).readValueAs(FieldNode.class));
  }
}