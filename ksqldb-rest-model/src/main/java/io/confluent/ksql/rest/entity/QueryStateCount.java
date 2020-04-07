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

import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;

import org.apache.kafka.streams.KafkaStreams;

/**
 * Used to keep track of a the state of KafkaStreams application
 * across multiple servers. Used in {@link RunningQuery}.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class QueryStateCount {
  
  // Use a EnumMap so toString() will always return the same string
  private final EnumMap<KafkaStreams.State, Integer> states;

  public QueryStateCount() {
    this.states = returnEnumMap();
  }

  @SuppressWarnings("unused") // Invoked by reflection
  @JsonCreator
  public QueryStateCount(final Map<KafkaStreams.State, Integer> states) {
    this.states = states.isEmpty() ? returnEnumMap() : new EnumMap<>(states);
  }

  
  public void updateStateCount(final String state, final int change) {
    updateStateCount(KafkaStreams.State.valueOf(state), change);
  }

  public void updateStateCount(final KafkaStreams.State state, final int change) {
    this.states.compute(state, (key, existing) ->
        existing == null
            ? change
            : existing + change);
    
  }

  @JsonValue
  public Map<KafkaStreams.State, Integer> getStates() {
    return Collections.unmodifiableMap(states);
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
    return Objects.equals(states, that.states);
  }

  @Override
  public int hashCode() {
    return Objects.hash(states);
  }

  @Override
  public String toString() {
    return Joiner.on(",").withKeyValueSeparator(":").join(this.states);
  }
  
  private static EnumMap<KafkaStreams.State, Integer> returnEnumMap() {
    return new EnumMap<>(KafkaStreams.State.class);
  }
}