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
import java.util.function.Function;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.kstream.SessionWindowedDeserializer;
import org.apache.kafka.streams.kstream.SessionWindowedSerializer;
import org.apache.kafka.streams.kstream.TimeWindowedDeserializer;
import org.apache.kafka.streams.kstream.TimeWindowedSerializer;
import org.apache.kafka.streams.kstream.Window;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.SessionWindow;
import org.apache.kafka.streams.kstream.internals.TimeWindow;

public class Record {

  final Topic topic;
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
      final long timestamp,
      final WindowData window
  ) {
    this(topic, key, value, jsonValue, Optional.of(timestamp), window);
  }

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

  Serializer<?> keySerializer() {
    final Serializer<String> stringDe = Serdes.String().serializer();
    if (window == null) {
      return stringDe;
    }

    return window.type == WindowData.Type.SESSION
        ? new SessionWindowedSerializer<>(stringDe)
        : new TimeWindowedSerializer<>(stringDe);
  }

  @SuppressWarnings("rawtypes")
  Deserializer keyDeserializer() {
    if (window == null) {
      return Serdes.String().deserializer();
    }

    final Deserializer<String> inner = Serdes.String().deserializer();
    return window.type == WindowData.Type.SESSION
        ? new SessionWindowedDeserializer<>(inner)
        : new TimeWindowedDeserializer<>(inner, window.size());
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

  /**
   * Coerce the key value to the correct type.
   *
   * <p>The type of the key loaded from the JSON test case file may not be the exact match on type,
   * e.g. JSON will load a small number as an integer, but the key type of the source might be a
   * long.
   *
   * @param keyCoercer function to coerce the key to the right type
   * @return a new Record with the correct key type.
   */
  public Record coerceKey(final Function<Object, Object> keyCoercer) {
    return new Record(
        topic,
        keyCoercer.apply(key),
        value,
        jsonValue.orElse(null),
        timestamp,
        window
    );
  }
}