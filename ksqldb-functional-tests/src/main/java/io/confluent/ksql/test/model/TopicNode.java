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

package io.confluent.ksql.test.model;

import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.TextNode;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.ksql.test.tools.TestJsonMapper;
import io.confluent.ksql.test.tools.Topic;
import io.confluent.ksql.test.tools.exceptions.InvalidFieldException;
import io.confluent.ksql.test.utils.SerdeUtil;
import java.util.Optional;

@JsonInclude(Include.NON_EMPTY)
public final class TopicNode {

  // Mapper used to parse schemas:
  private static final ObjectMapper OBJECT_MAPPER = TestJsonMapper.INSTANCE.get();

  private final String name;
  private final JsonNode schema;
  private final int numPartitions;
  private final int replicas;
  private final String format;

  public TopicNode(
      @JsonProperty("name") final String name,
      @JsonProperty("schema") final JsonNode schema,
      @JsonProperty("format") final String format,
      @JsonProperty("partitions") final Integer numPartitions,
      @JsonProperty("replicas") final Integer replicas
  ) {
    this.name = name == null ? "" : name;
    this.schema = schema == null ? NullNode.getInstance() : schema;
    this.format = format;
    this.numPartitions = numPartitions == null ? 1 : numPartitions;
    this.replicas = replicas == null ? 1 : replicas;

    if (this.name.isEmpty()) {
      throw new InvalidFieldException("name", "empty or missing");
    }

    // Fail early:
    buildSchema();
  }

  public String getName() {
    return name;
  }

  public JsonNode getSchema() {
    return schema instanceof NullNode ? null : schema;
  }

  public String getFormat() {
    return format;
  }

  public int getNumPartitions() {
    return numPartitions;
  }

  public int getReplicas() {
    return replicas;
  }

  public Topic build() {
    return new Topic(name, numPartitions, replicas, buildSchema());
  }

  public static TopicNode from(final Topic topic) {
    final String format = topic.getSchema()
        .map(ParsedSchema::schemaType)
        .orElse(null);

    return new TopicNode(
        topic.getName(),
        topic.getSchema()
            .map(schema -> buildSchemaNode(schema, format))
            .orElseGet(NullNode::getInstance),
        format,
        topic.getNumPartitions(),
        (int) topic.getReplicas()
    );
  }

  private static JsonNode buildSchemaNode(final ParsedSchema schema, final String format) {
    final String canonical = schema.canonicalString();

    try {
      if ("PROTOBUF".equals(format)) {
        return new TextNode(canonical);
      }

      return OBJECT_MAPPER.readTree(canonical);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  private Optional<ParsedSchema> buildSchema() {
    return SerdeUtil.buildSchema(schema, format);
  }
}