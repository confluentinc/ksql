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

package io.confluent.ksql.test.serde;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import io.confluent.ksql.parser.tree.Type;
import io.confluent.ksql.schema.ksql.SchemaConverters;
import io.confluent.ksql.schema.ksql.TypeContextUtil;
import io.confluent.ksql.test.model.KeyFieldNode;
import io.confluent.ksql.test.tools.exceptions.InvalidFieldException;
import java.io.IOException;
import java.util.Optional;
import org.apache.kafka.connect.data.Schema;

public class KeyFieldDeserializer extends StdDeserializer<KeyFieldNode> {

  public KeyFieldDeserializer() {
    super(KeyFieldNode.class);
  }

  @Override
  public KeyFieldNode deserialize(
      final JsonParser jp,
      final DeserializationContext ctxt
  ) throws IOException {

    final JsonNode node = jp.getCodec().readTree(jp);

    final Optional<String> name = buildString("name", node, jp);
    final Optional<String> legacyName = buildString("legacyName", node, jp);
    final Optional<Schema> legacySchema = buildLegacySchema(node, jp);

    return new KeyFieldNode(name, legacyName, legacySchema);
  }

  private static Optional<String> buildString(
      final String name,
      final JsonNode node,
      final JsonParser jp
  ) throws IOException {
    if (!node.has(name)) {
      return KeyFieldNode.EXCLUDE_NAME;
    }

    final String value = node
        .get(name)
        .traverse(jp.getCodec())
        .readValueAs(String.class);

    return Optional.ofNullable(value);
  }

  private static Optional<Schema> buildLegacySchema(
      final JsonNode node,
      final JsonParser jp
  ) throws IOException {
    if (!node.has("legacySchema")) {
      return KeyFieldNode.EXCLUDE_SCHEMA;
    }

    final String valueSchema = node
        .get("legacySchema")
        .traverse(jp.getCodec())
        .readValueAs(String.class);

    try {
      return Optional.ofNullable(valueSchema)
          .map(TypeContextUtil::getType)
          .map(Type::getSqlType)
          .map(SchemaConverters.sqlToLogicalConverter()::fromSqlType);
    } catch (final Exception e) {
      throw new InvalidFieldException("legacySchema", "Failed to parse: " + valueSchema, e);
    }
  }
}