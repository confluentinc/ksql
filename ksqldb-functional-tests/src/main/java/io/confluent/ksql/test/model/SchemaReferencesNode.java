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

package io.confluent.ksql.test.model;

import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.TextNode;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.ksql.serde.protobuf.ProtobufFormat;
import io.confluent.ksql.test.tools.TestJsonMapper;
import io.confluent.ksql.test.utils.SerdeUtil;
import io.confluent.ksql.tools.test.model.SchemaReference;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
public final class SchemaReferencesNode {
  // Mapper used to parse schemas:
  private static final ObjectMapper OBJECT_MAPPER = TestJsonMapper.INSTANCE.get();

  private final String name;
  private final String format;
  private final JsonNode schema;

  public SchemaReferencesNode(
      @JsonProperty("name") final String name,
      @JsonProperty("format") final String format,
      @JsonProperty("schema") final JsonNode schema
  ) {
    this.name = name;
    this.format = requireNonNull(format, "format");
    this.schema = requireNonNull(schema, "schema");

    // Fail early:
    SerdeUtil.buildSchema(schema, format);
  }

  public String getName() {
    return name;
  }

  public String getFormat() {
    return format;
  }

  @JsonInclude(JsonInclude.Include.NON_NULL)
  public JsonNode getSchema() {
    return schema;
  }

  public SchemaReference build() {
    return new SchemaReference(
        name,
        SerdeUtil.buildSchema(schema, format).get()
    );
  }

  public static SchemaReferencesNode from(final SchemaReference schemaReferences) {
    return new SchemaReferencesNode(
        schemaReferences.getName(),
        schemaReferences.getSchema().schemaType(),
        buildSchemaNode(schemaReferences.getSchema())
    );
  }

  private static JsonNode buildSchemaNode(final ParsedSchema schema) {
    final String canonical = schema.canonicalString();

    try {
      if (schema.schemaType().equals(ProtobufFormat.NAME)) {
        return new TextNode(canonical);
      }

      return OBJECT_MAPPER.readTree(canonical);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }
}
