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

import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;
import io.confluent.ksql.test.model.TestHeader;
import io.confluent.ksql.test.model.WindowData;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.streams.kstream.Window;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.SessionWindow;
import org.apache.kafka.streams.kstream.internals.TimeWindow;

public class Record {

  private final String topicName;
  private final Object key;
  private final Object value;
  private final Optional<Long> timestamp;
  private final WindowData window;
  private final Optional<JsonNode> jsonValue;
  private final Optional<JsonNode> jsonKey;
  private final Optional<List<TestHeader>> headers;

  public Record(
      final String topicName,
      final Object key,
      final JsonNode jsonKey,
      final Object value,
      final JsonNode jsonValue,
      final Optional<Long> timestamp,
      final WindowData window,
      final Optional<List<TestHeader>> headers
  ) {
    this.topicName = requireNonNull(topicName, "topicName");
    this.key = key;
    this.jsonKey = Optional.ofNullable(jsonKey);
    this.value = value;
    this.jsonValue = Optional.ofNullable(jsonValue);
    this.timestamp = requireNonNull(timestamp, "timestamp");
    this.window = window;
    this.headers = requireNonNull(headers);

    if (!topicName.trim().equals(topicName)) {
      throw new IllegalArgumentException("Record topic names must not start or end with whitespace:"
          + " '" + topicName + "'");
    }

    if (topicName.isEmpty()) {
      throw new IllegalArgumentException("Record topic name can not be empty");
    }
  }

  public String getTopicName() {
    return topicName;
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

  public Optional<JsonNode> getJsonKey() {
    return jsonKey;
  }

  public Optional<JsonNode> getJsonValue() {
    return jsonValue;
  }

  /**
   * @return expected headers, or {@link Optional#empty()} if headers can be anything.
   */
  public Optional<List<TestHeader>> headers() {
    return headers;
  }

  /**
   * @return expected headers, or {@link Optional#empty()} if headers can be anything.
   */
  public Optional<List<Header>> headersAsHeaders() {
    return headers.map(ArrayList::new);
  }

  public Record withKeyValue(final Object key, final Object value) {
    return new Record(
        topicName,
        key,
        jsonKey.orElse(null),
        value,
        jsonValue.orElse(null),
        timestamp,
        window,
        headers
    );
  }

  @SuppressWarnings("unchecked")
  public ProducerRecord<Object, Object> asProducerRecord() {
    return new ProducerRecord(
        topicName,
        0,
        timestamp.orElse(0L),
        key(),
        value,
        headers.orElse(ImmutableList.of())
    );
  }
}