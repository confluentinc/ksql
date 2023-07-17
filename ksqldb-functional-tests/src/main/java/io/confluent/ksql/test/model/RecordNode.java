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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.ksql.schema.ksql.SimpleColumn;
import io.confluent.ksql.serde.SchemaTranslator;
import io.confluent.ksql.serde.SerdeFeatures;
import io.confluent.ksql.serde.avro.AvroFormat;
import io.confluent.ksql.serde.protobuf.ProtobufFormat;
import io.confluent.ksql.test.tools.Record;
import io.confluent.ksql.test.tools.exceptions.InvalidFieldException;
import io.confluent.ksql.test.tools.exceptions.MissingFieldException;
import io.confluent.ksql.test.utils.JsonParsingUtil;
import io.confluent.ksql.util.BytesUtils;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@JsonDeserialize(using = RecordNode.Deserializer.class)
@JsonSerialize(using = RecordNode.Serializer.class)
public final class RecordNode {

  private static final ObjectMapper objectMapper = new ObjectMapper()
      .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
      .enable(JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN)
      .setNodeFactory(JsonNodeFactory.withExactBigDecimals(true));

  private static final SchemaTranslator protobufSchemaTranslator =
      (new ProtobufFormat()).getSchemaTranslator(ImmutableMap.of());
  private static final SchemaTranslator avroSchemaTranslator =
      (new AvroFormat()).getSchemaTranslator(ImmutableMap.of());

  private final String topicName;
  private final JsonNode key;
  private final JsonNode value;
  private final Optional<Long> timestamp;
  private final Optional<WindowData> window;
  private final Optional<List<TestHeader>> headers;

  @VisibleForTesting
  RecordNode(
      final String topicName,
      final JsonNode key,
      final JsonNode value,
      final Optional<Long> timestamp,
      final Optional<WindowData> window,
      final Optional<List<TestHeader>> headers
  ) {
    this.topicName = topicName == null ? "" : topicName;
    this.key = requireNonNull(key, "key");
    this.value = requireNonNull(value, "value");
    this.timestamp = requireNonNull(timestamp, "timestamp");
    this.window = requireNonNull(window, "window");
    this.headers = requireNonNull(headers, "headers");

    if (this.topicName.isEmpty()) {
      throw new MissingFieldException("topic");
    }
  }

  public String topicName() {
    return topicName;
  }

  public Record build() {
    return build(Optional.empty(), Optional.empty(), SerdeFeatures.of(), SerdeFeatures.of());
  }

  public Record build(
      final Optional<ParsedSchema> keySchema,
      final Optional<ParsedSchema> valueSchema,
      final SerdeFeatures keyFeatures,
      final SerdeFeatures valueFeatures
  ) {
    final Object recordKey = buildJson(key);
    final Object recordValue = buildJson(value);

    return new Record(
        topicName,
        keySchema.map(s -> coerceRecord(recordKey, s, true, keyFeatures)).orElse(recordKey),
        key,
        valueSchema.map(s -> coerceRecord(recordValue, s, false, valueFeatures))
            .orElse(recordValue),
        value,
        timestamp,
        window.orElse(null),
        headers
    );
  }

  public static RecordNode from(final Record record) {
    return new RecordNode(
        record.getTopicName(),
        record.getJsonKey().orElse(NullNode.getInstance()),
        record.getJsonValue().orElse(NullNode.getInstance()),
        record.timestamp(),
        Optional.ofNullable(record.getWindow()),
        record.headers()
    );
  }

  public Optional<WindowData> getWindow() {
    return window;
  }

  private Object buildJson(final JsonNode contents) {
    if (contents instanceof NullNode) {
      return null;
    }

    if (contents instanceof TextNode) {
      return contents.asText();
    }

    try {
      return objectMapper.readValue(objectMapper.writeValueAsString(contents), Object.class);
    } catch (final IOException e) {
      throw new InvalidFieldException("value", "failed to parse", e);
    }
  }

  @SuppressWarnings("unchecked")
  private Object coerceRecord(
      final Object record,
      final ParsedSchema schema,
      final boolean isKey,
      final SerdeFeatures features
  ) {
    if (!(record instanceof Map)) {
      return record;
    }

    final Map<String, Object> recordMap = (Map<String, Object>) record;

    final List<SimpleColumn> columns;
    switch (schema.schemaType()) {
      case ProtobufFormat.NAME:
        columns = protobufSchemaTranslator.toColumns(schema, features, isKey);
        break;
      case AvroFormat.NAME:
        columns = avroSchemaTranslator.toColumns(schema, features, isKey);
        break;
      default:
        return record;
    }

    columns.forEach(c -> coerceColumn(recordMap, c));
    return recordMap;
  }

  private void coerceColumn(final Map<String, Object> record, final SimpleColumn column) {
    final String lowerCaseName = column.name().text().toLowerCase();
    final String upperCaseName = column.name().text().toUpperCase();
    final String name = (record.containsKey(lowerCaseName)) ? lowerCaseName : upperCaseName;

    final Object value = record.get(name);
    if (value == null) {
      return;
    }

    switch (column.type().baseType()) {
      case BYTES:
        if (value instanceof String) {
          record.put(name, BytesUtils.decode((String) value, BytesUtils.Encoding.BASE64));
        }

        break;
      default:
    }
  }

  public static class Deserializer extends JsonDeserializer<RecordNode> {

    @Override
    public RecordNode deserialize(
        final JsonParser jp,
        final DeserializationContext ctxt
    ) throws IOException {
      final JsonNode node = jp.getCodec().readTree(jp);

      final String topic = JsonParsingUtil.getRequired("topic", node, jp, String.class);

      final Optional<JsonNode> key = JsonParsingUtil.getOptional("key", node, jp, JsonNode.class);

      final JsonNode value = JsonParsingUtil.getRequired("value", node, jp, JsonNode.class);

      final Optional<Long> timestamp = JsonParsingUtil
          .getOptional("timestamp", node, jp, Long.class);

      final Optional<WindowData> window = JsonParsingUtil
          .getOptional("window", node, jp, WindowData.class);

      final Optional<List<TestHeader>> headers = JsonParsingUtil
          .getOptional("headers", node, jp, new TypeReference<List<TestHeader>>() {});

      return new RecordNode(
          topic, key.orElse(NullNode.getInstance()), value, timestamp, window, headers);
    }
  }

  public static class Serializer extends JsonSerializer<RecordNode> {

    @Override
    public void serialize(
        final RecordNode record,
        final JsonGenerator jsonGenerator,
        final SerializerProvider serializerProvider
    ) throws IOException {
      jsonGenerator.writeStartObject();
      jsonGenerator.writeStringField("topic", record.topicName);
      jsonGenerator.writeObjectField("key", record.key);
      jsonGenerator.writeObjectField("value", record.value);
      if (record.timestamp.isPresent()) {
        jsonGenerator.writeNumberField("timestamp", record.timestamp.get());
      }
      if (record.window.isPresent()) {
        jsonGenerator.writeObjectField("window", record.window);
      }
      if (record.headers.isPresent()) {
        jsonGenerator.writeObjectField("headers", record.headers.get());
      }
      jsonGenerator.writeEndObject();
    }
  }
}