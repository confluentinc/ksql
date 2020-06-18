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
import com.fasterxml.jackson.databind.DeserializationContext;
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
import io.confluent.ksql.test.tools.Record;
import io.confluent.ksql.test.tools.exceptions.InvalidFieldException;
import io.confluent.ksql.test.tools.exceptions.MissingFieldException;
import io.confluent.ksql.test.utils.JsonParsingUtil;
import java.io.IOException;
import java.util.Optional;

@JsonDeserialize(using = RecordNode.Deserializer.class)
@JsonSerialize(using = RecordNode.Serializer.class)
public final class RecordNode {

  private static final ObjectMapper objectMapper = new ObjectMapper()
      .setNodeFactory(JsonNodeFactory.withExactBigDecimals(true));

  private final String topicName;
  private final Optional<Object> key;
  private final JsonNode value;
  private final Optional<Long> timestamp;
  private final Optional<WindowData> window;

  private RecordNode(
      final String topicName,
      final Optional<Object> key,
      final JsonNode value,
      final Optional<Long> timestamp,
      final Optional<WindowData> window
  ) {
    this.topicName = topicName == null ? "" : topicName;
    this.key = requireNonNull(key, "key");
    this.value = requireNonNull(value, "value");
    this.timestamp = requireNonNull(timestamp, "timestamp");
    this.window = requireNonNull(window, "window");

    if (this.topicName.isEmpty()) {
      throw new MissingFieldException("topic");
    }
  }

  public String topicName() {
    return topicName;
  }

  public Record build() {
    final Object recordValue = buildValue();

    return new Record(
        topicName,
        key.orElse(null),
        recordValue,
        value,
        timestamp,
        window.orElse(null)
    );
  }

  public static RecordNode from(final Record record) {
    return new RecordNode(
        record.getTopicName(),
        Optional.ofNullable(record.rawKey()),
        record.getJsonValue().orElse(NullNode.getInstance()),
        record.timestamp(),
        Optional.ofNullable(record.getWindow())
    );
  }

  public Optional<WindowData> getWindow() {
    return window;
  }

  private Object buildValue() {
    if (value instanceof NullNode) {
      return null;
    }

    if (value instanceof TextNode) {
      return value.asText();
    }

    try {
      return objectMapper.readValue(objectMapper.writeValueAsString(value), Object.class);
    } catch (final IOException e) {
      throw new InvalidFieldException("value", "failed to parse", e);
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

      final Optional<Object> key = JsonParsingUtil.getOptional("key", node, jp, Object.class);

      final JsonNode value = JsonParsingUtil.getRequired("value", node, jp, JsonNode.class);

      final Optional<Long> timestamp = JsonParsingUtil
          .getOptional("timestamp", node, jp, Long.class);

      final Optional<WindowData> window = JsonParsingUtil
          .getOptional("window", node, jp, WindowData.class);

      return new RecordNode(topic, key, value, timestamp, window);
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
      if (record.key.isPresent()) {
        jsonGenerator.writeObjectField("key", record.key);
      } else {
        jsonGenerator.writeNullField("key");
      }
      jsonGenerator.writeObjectField("value", record.value);
      if (record.timestamp.isPresent()) {
        jsonGenerator.writeNumberField("timestamp", record.timestamp.get());
      }
      if (record.window.isPresent()) {
        jsonGenerator.writeObjectField("window", record.window);
      }
      jsonGenerator.writeEndObject();
    }
  }
}