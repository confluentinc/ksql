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

import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;

public class RecordNode {

  private static final ObjectMapper objectMapper = new ObjectMapper();

  private final String topicName;
  private final String key;
  private final JsonNode value;
  private final long timestamp;
  private final Optional<WindowData> window;

  RecordNode(
      @JsonProperty("topic") final String topicName,
      @JsonProperty("key") final String key,
      @JsonProperty("value") final JsonNode value,
      @JsonProperty("timestamp") final Long timestamp,
      @JsonProperty("window") final WindowData window
  ) {
    this.topicName = topicName == null ? "" : topicName;
    this.key = key == null ? "" : key;
    this.value = requireNonNull(value, "value");
    this.timestamp = timestamp == null ? 0L : timestamp;
    this.window = Optional.ofNullable(window);

    if (this.topicName.isEmpty()) {
      throw new MissingFieldException("topic");
    }
  }

  public String topicName() {
    return topicName;
  }

  Record build(final Map<String, Topic> topics) {
    final Topic topic = topics.get(topicName());

    final Object topicValue = buildValue(topic);

    return new Record(
        topic,
        key,
        topicValue,
        timestamp,
        window.orElse(null)
    );
  }

  private Object buildValue(final Topic topic) {
    if (value.asText().equals("null")) {
      return null;
    }

    if (topic.getSerdeSupplier() instanceof StringSerdeSupplier) {
      return value.asText();
    }

    try {
      return objectMapper.readValue(objectMapper.writeValueAsString(value), Object.class);
    } catch (final IOException e) {
      throw new InvalidFieldException("value", "failed to parse", e);
    }
  }
}