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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.TextNode;
import io.confluent.ksql.test.tools.Record;
import io.confluent.ksql.test.tools.Topic;
import io.confluent.ksql.test.tools.exceptions.InvalidFieldException;
import io.confluent.ksql.test.tools.exceptions.MissingFieldException;
import io.confluent.ksql.test.utils.JsonParsingUtil;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;

@JsonDeserialize(using = RecordNode.Deserializer.class)
public final class RecordNode {

  private static final ObjectMapper objectMapper = new ObjectMapper();

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

  public Record build(final Map<String, Topic> topics) {
    final Topic topic = topics.get(topicName);

    final Object recordValue = buildValue();

    return new Record(
        topic,
        key.orElse(null),
        recordValue,
        value,
        timestamp,
        window.orElse(null)
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

      final Optional<Object> key = node.has("key")
          ? JsonParsingUtil.getOptional("key", node, jp, Object.class)
          : Optional.of("");

      final JsonNode value = JsonParsingUtil.getRequired("value", node, jp, JsonNode.class);

      final Optional<Long> timestamp = JsonParsingUtil
          .getOptional("timestamp", node, jp, Long.class);

      final Optional<WindowData> window = JsonParsingUtil
          .getOptional("window", node, jp, WindowData.class);

      return new RecordNode(topic, key, value, timestamp, window);
    }
  }
}