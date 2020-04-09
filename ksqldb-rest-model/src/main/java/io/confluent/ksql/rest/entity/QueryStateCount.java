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

import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.KsqlConstants.KsqlQueryState;
import java.util.Collections;
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
  private final EnumMap<KsqlQueryState, Integer> states;

  public QueryStateCount() {
    this.states = returnEnumMap();
  }

  @SuppressWarnings("unused") // Invoked by reflection
  @JsonCreator
  public QueryStateCount(final Map<KafkaStreams.State, Integer> states) {
    final Map<KsqlQueryState, Integer> ksqlQueryStates = states.entrySet().stream()
        .collect(Collectors.toMap(
            e -> fromStreamsState(e.getKey()),
            Map.Entry::getValue));
    this.states = states.isEmpty() ? returnEnumMap() : new EnumMap<>(ksqlQueryStates);
  }

  
  public void updateStateCount(final String state, final int change) {
    updateStateCount(KafkaStreams.State.valueOf(state), change);
  }

  public void updateStateCount(final KafkaStreams.State state, final int change) {
    updateStateCount(fromStreamsState(state), change);
  }

  public void updateStateCount(final KsqlQueryState state, final int change) {
    this.states.compute(state, (key, existing) ->
        existing == null
            ? change
            : existing + change);
  }

  @JsonValue
  public Map<KsqlQueryState, Integer> getStates() {
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

  public static KsqlQueryState fromStreamsState(final KafkaStreams.State state) {
    return state == KafkaStreams.State.ERROR ? KsqlQueryState.ERROR : KsqlQueryState.RUNNING;
  }
  
  private static EnumMap<KsqlQueryState, Integer> returnEnumMap() {
    return new EnumMap<>(KsqlQueryState.class);
  }
}