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

package io.confluent.ksql.test.commons;

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
  final String key;
  final Object value;
  final long timestamp;
  final WindowData window;

  public Record(final Topic topic,
      final String key,
      final Object value,
      final long timestamp,
      final WindowData window) {
    this.topic = topic;
    this.key = key;
    this.value = value;
    this.timestamp = timestamp;
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

  @SuppressWarnings("unchecked")
  Deserializer keyDeserializer() {
    if (window == null) {
      return Serdes.String().deserializer();
    }

    final Deserializer<String> inner = Serdes.String().deserializer();
    return window.type == WindowData.Type.SESSION
        ? new SessionWindowedDeserializer<>(inner)
        : new TimeWindowedDeserializer<>(inner, window.size());
  }

  @SuppressWarnings("unchecked")
  public <W> W key() {
    if (window == null) {
      return (W) key;
    }

    final Window w = window.type == WindowData.Type.SESSION
        ? new SessionWindow(this.window.start, this.window.end)
        : new TimeWindow(this.window.start, this.window.end);
    return (W) new Windowed<>(key, w);
  }

  public Object value() {
    return value;
  }

  public long timestamp() {
    return timestamp;
  }
}