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
import io.confluent.ksql.util.KsqlException;

import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;

import org.apache.kafka.streams.KafkaStreams;

/**
 * Used to keep track of a the state of KafkaStreams application
 * across multiple servers. Used in {@link RunningQuery}.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class KafkaStreamsStateCount {
  
  // Use a EnumMap so toString() will always return the same string
  private final EnumMap<KafkaStreams.State, Integer> state;

  public KafkaStreamsStateCount() {
    this.state = returnEnumMap();
  }

  @JsonCreator
  public KafkaStreamsStateCount(final String serializedPair) {
    final String [] parts = serializedPair.split(",");
    final EnumMap<KafkaStreams.State, Integer> deserializedKafkaStreamsStateCount = returnEnumMap();
    for (String stateCount : parts) {
      final String[] split = stateCount.split(":");
      if (split.length != 2) {
        throw new KsqlException(
            "Invalid state count info. Expected format: <KafkaStreams.State>:<count>, but was "
                + serializedPair);
      }

      final String currentState = split[0].trim();

      try {
        final int count = Integer.parseInt(split[1]);
        deserializedKafkaStreamsStateCount.put(
            KafkaStreams.State.valueOf(currentState), count);
      } catch (final Exception e) {
        throw new KsqlException(
            "Invalid count. Expected format: <KafkaStreams.State>:<count>, but was "
                + serializedPair, e);
      }
    }

    this.state = deserializedKafkaStreamsStateCount;
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

    final KafkaStreamsStateCount that = (KafkaStreamsStateCount) o;
    return Objects.equals(state, that.state);
  }

  @Override
  public int hashCode() {
    return Objects.hash(state, state);
  }

  @JsonValue
  @Override
  public String toString() {
    final StringBuilder output = new StringBuilder();
    this.state.forEach((kafkaStreamsState, count) -> {
      output.append(kafkaStreamsState.toString()).append(":").append(count).append(", ");
    });
    output.delete(output.length() - 2, output.length());
    return output.toString();
  }
  
  private static EnumMap<KafkaStreams.State, Integer> returnEnumMap() {
    return new EnumMap<>(KafkaStreams.State.class);
  }
}