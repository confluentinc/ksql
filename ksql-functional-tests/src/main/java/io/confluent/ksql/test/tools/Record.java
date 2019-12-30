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

package io.confluent.ksql.test.tools;

import com.fasterxml.jackson.databind.JsonNode;
import io.confluent.ksql.test.model.WindowData;
import java.util.Objects;
import java.util.Optional;
import org.apache.kafka.streams.kstream.Window;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.SessionWindow;
import org.apache.kafka.streams.kstream.internals.TimeWindow;

public class Record {

  private final Topic topic;
  private final Object key;
  private final Object value;
  private final Optional<Long> timestamp;
  private final WindowData window;
  private final Optional<JsonNode> jsonValue;

  public Record(
      final Topic topic,
      final Object key,
      final Object value,
      final JsonNode jsonValue,
      final Optional<Long> timestamp,
      final WindowData window
  ) {
    this.topic = topic;
    this.key = key;
    this.value = value;
    this.jsonValue = Optional.ofNullable(jsonValue);
    this.timestamp = Objects.requireNonNull(timestamp, "timestamp");
    this.window = window;
  }

  public Topic getTopic() {
    return topic;
  }

  public Object rawKey() {
    return key;
  }

  public Object key() {
    if (window == null) {
      return key;
    }

    final Window w = window.type == WindowData.Type.SESSION
        ? new SessionWindow(this.window.start, this.window.end)
        : new TimeWindow(this.window.start, this.window.end);
    return new Windowed<>(key, w);
  }

  public Object value() {
    return value;
  }

  /**
   * @return expected timestamp, or {@link Optional#empty()} if timestamp can be anything.
   */
  public Optional<Long> timestamp() {
    return timestamp;
  }

  public WindowData getWindow() {
    return window;
  }

  public Topic topic() {
    return topic;
  }

  public Optional<JsonNode> getJsonValue() {
    return jsonValue;
  }

  public Record withKey(final Object key) {
    return new Record(
        topic,
        key,
        value,
        jsonValue.orElse(null),
        timestamp,
        window
    );
  }
}