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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.google.common.collect.ImmutableList;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.ksql.serde.SerdeFeatures;
import io.confluent.ksql.serde.protobuf.ProtobufFormat;
import io.confluent.ksql.test.tools.TestJsonMapper;
import io.confluent.ksql.test.tools.exceptions.InvalidFieldException;
import io.confluent.ksql.test.utils.SerdeUtil;
import io.confluent.ksql.tools.test.model.SchemaReference;
import io.confluent.ksql.tools.test.model.Topic;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@JsonInclude(Include.NON_EMPTY)
public final class TopicNode {

  // Mapper used to parse schemas:
  private static final ObjectMapper OBJECT_MAPPER = TestJsonMapper.INSTANCE.get();

  private final String name;
  private final Optional<Integer> keySchemaId;
  private final Optional<Integer> valueSchemaId;
  private final JsonNode keySchema;
  private final JsonNode valueSchema;
  private final List<SchemaReferencesNode> keySchemaReferences;
  private final List<SchemaReferencesNode> valueSchemaReferences;
  private final int numPartitions;
  private final int replicas;
  private final String keyFormat;
  private final String valueFormat;
  private final SerdeFeatures keySerdeFeatures;
  private final SerdeFeatures valueSerdeFeatures;

  // CHECKSTYLE_RULES.OFF: NPathComplexity
  // CHECKSTYLE_RULES.OFF: ParameterNumberCheck
  public TopicNode(
      @JsonProperty("name") final String name,
      @JsonProperty("keySchemaId") final Optional<Integer> keySchemaId,
      @JsonProperty("valueSchemaId") final Optional<Integer> valueSchemaId,
      @JsonProperty("keySchema") final JsonNode keySchema,
      @JsonProperty("valueSchema") final JsonNode valueSchema,
      @JsonProperty("keySchemaReferences") final List<SchemaReferencesNode> keySchemaReferences,
      @JsonProperty("valueSchemaReferences") final List<SchemaReferencesNode> valueSchemaReferences,
      @JsonProperty("keyFormat") final String keyFormat,
      @JsonProperty("valueFormat") final String valueFormat,
      @JsonProperty("partitions") final Integer numPartitions,
      @JsonProperty("replicas") final Integer replicas,
      @JsonProperty("keySerdeFeatures") final SerdeFeatures keySerdeFeatures,
      @JsonProperty("valueSerdeFeatures") final SerdeFeatures valueSerdeFeatures
  ) {
    // CHECKSTYLE_RULES.ON: NPathComplexity
    // CHECKSTYLE_RULES.ON: ParameterNumberCheck

    this.name = name == null ? "" : name;
    this.keySchemaId = keySchemaId;
    this.valueSchemaId = valueSchemaId;
    this.keySchema = keySchema;
    this.valueSchema = valueSchema;
    this.keySchemaReferences = keySchemaReferences == null
        ? ImmutableList.of()
        : ImmutableList.copyOf(keySchemaReferences);
    this.valueSchemaReferences = valueSchemaReferences == null
        ? ImmutableList.of()
        : ImmutableList.copyOf(valueSchemaReferences);
    this.keyFormat = keyFormat;
    this.valueFormat = valueFormat;
    this.numPartitions = numPartitions == null ? 1 : numPartitions;
    this.replicas = replicas == null ? 1 : replicas;
    this.keySerdeFeatures = keySerdeFeatures == null ? SerdeFeatures.of() : keySerdeFeatures;
    this.valueSerdeFeatures = valueSerdeFeatures == null ? SerdeFeatures.of() : valueSerdeFeatures;

    if (this.name.isEmpty()) {
      throw new InvalidFieldException("name", "empty or missing");
    }

    // Fail early:
    SerdeUtil.buildSchema(keySchema, keyFormat);
    SerdeUtil.buildSchema(valueSchema, valueFormat);
  }

  public String getName() {
    return name;
  }

  public Optional<Integer> getKeySchemaId() {
    return keySchemaId;
  }

  public Optional<Integer> getValueSchemaId() {
    return valueSchemaId;
  }

  @SuppressFBWarnings(value = "EI_EXPOSE_REP",
      justification = "keySchemaReferences is ImmutableList")
  public List<SchemaReferencesNode> getKeySchemaReferences() {
    return keySchemaReferences;
  }

  @SuppressFBWarnings(value = "EI_EXPOSE_REP",
      justification = "valueSchemaReferences is ImmutableList")
  public List<SchemaReferencesNode> getValueSchemaReferences() {
    return valueSchemaReferences;
  }

  @JsonInclude(Include.NON_NULL)
  public JsonNode getKeySchema() {
    return keySchema instanceof NullNode ? null : keySchema;
  }

  @JsonInclude(Include.NON_NULL)
  public JsonNode getValueSchema() {
    return valueSchema instanceof NullNode ? null : valueSchema;
  }

  public String getKeyFormat() {
    return keyFormat;
  }

  public String getValueFormat() {
    return valueFormat;
  }

  public int getNumPartitions() {
    return numPartitions;
  }

  public int getReplicas() {
    return replicas;
  }

  public SerdeFeatures getKeySerdeFeatures() {
    return keySerdeFeatures;
  }

  public SerdeFeatures getValueSerdeFeatures() {
    return valueSerdeFeatures;
  }

  public Topic build() {
    final List<SchemaReference> keyReferences = keySchemaReferences.stream()
        .map(SchemaReferencesNode::build)
        .collect(Collectors.toList());

    final List<SchemaReference> valueReferences = valueSchemaReferences.stream()
        .map(SchemaReferencesNode::build)
        .collect(Collectors.toList());

    return new Topic(
        name,
        numPartitions,
        replicas,
        keySchemaId,
        valueSchemaId,
        SerdeUtil.buildSchema(keySchema, keyFormat)
            .map(schema -> SerdeUtil.withSchemaReferences(schema, keyReferences)),
        SerdeUtil.buildSchema(valueSchema, valueFormat)
            .map(schema -> SerdeUtil.withSchemaReferences(schema, valueReferences)),
        keyReferences,
        valueReferences,
        keySerdeFeatures,
        valueSerdeFeatures
    );
  }

  public static TopicNode from(final Topic topic) {
    final String keyFormat = topic.getKeySchema()
        .map(ParsedSchema::schemaType)
        .orElse(null);
    final String valueFormat = topic.getValueSchema()
        .map(ParsedSchema::schemaType)
        .orElse(null);

    return new TopicNode(
        topic.getName(),
        topic.getKeySchemaId(),
        topic.getValueSchemaId(),
        topic.getKeySchema()
            .map(schema -> buildSchemaNode(schema, keyFormat))
            .orElseGet(NullNode::getInstance),
        topic.getValueSchema()
            .map(schema -> buildSchemaNode(schema, valueFormat))
            .orElseGet(NullNode::getInstance),
        topic.getKeySchemaReferences().stream().map(SchemaReferencesNode::from)
            .collect(Collectors.toList()),
        topic.getValueSchemaReferences().stream().map(SchemaReferencesNode::from)
            .collect(Collectors.toList()),
        keyFormat,
        valueFormat,
        topic.getNumPartitions(),
        (int) topic.getReplicas(),
        topic.getKeyFeatures(),
        topic.getValueFeatures()
    );
  }

  private static JsonNode buildSchemaNode(final ParsedSchema schema, final String format) {
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