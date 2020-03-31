/*
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.rest.entity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;

import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.kafka.streams.KafkaStreams;

/**
 * Used to keep track of a the state of KafkaStreams application
 * across multiple servers. Used in {@link RunningQuery}.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class QueryStateCount {
  
  // Use a EnumMap so toString() will always return the same string
  private final EnumMap<KafkaStreams.State, Integer> state;

  public QueryStateCount() {
    this.state = returnEnumMap();
  }

  @JsonCreator
  public QueryStateCount(final String serializedPair) {
    final Map<String, String> pairs =
        Splitter.on(",").withKeyValueSeparator(":").split(serializedPair);
    this.state = pairs.entrySet().stream().collect(
        Collectors.toMap(
            e -> KafkaStreams.State.valueOf(e.getKey().trim()),
            e -> Integer.parseInt(e.getValue()),
            (k1, k2) -> {
              throw new IllegalArgumentException("Duplicate key " + k2);
            },
            () -> new EnumMap<>(KafkaStreams.State.class)));
  }

  public void updateStateCount(final String state, final int change) {
    updateStateCount(KafkaStreams.State.valueOf(state), change);
  }

  public void updateStateCount(final KafkaStreams.State state, final int change) {
    final int newCount = this.state.getOrDefault(state, 0) + change;
    this.state.put(state, newCount);
    
  }

  public Map<KafkaStreams.State, Integer> getState() {
    return state;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final QueryStateCount that = (QueryStateCount) o;
    return Objects.equals(state, that.state);
  }

  @Override
  public int hashCode() {
    return Objects.hash(state);
  }

  @JsonValue
  @Override
  public String toString() {
    return Joiner.on(",").withKeyValueSeparator(":").join(this.state);
  }
  
  private static EnumMap<KafkaStreams.State, Integer> returnEnumMap() {
    return new EnumMap<>(KafkaStreams.State.class);
  }
}