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
import io.confluent.ksql.util.KsqlConstants.KsqlQueryStatus;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;
import org.apache.kafka.streams.KafkaStreams;

/**
 * Used to keep track of the state of a KafkaStreams application
 * across multiple servers. Used in {@link RunningQuery}.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class QueryStatusCount {
  
  // Use a EnumMap so toString() will always return the same string
  private final EnumMap<KsqlQueryStatus, Integer> statuses;

  public QueryStatusCount() {
    this(Collections.emptyMap());
  }

  @JsonCreator
  public QueryStatusCount(final Map<KsqlQueryStatus, Integer> states) {
    this.statuses = states.isEmpty() ? returnEnumMap() : new EnumMap<>(states);
  }

  public void updateStatusCount(final String state, final int change) {
    updateStatusCount(KafkaStreams.State.valueOf(state), change);
  }

  public void updateStatusCount(final KafkaStreams.State state, final int change) {
    updateStatusCount(KsqlConstants.fromStreamsState(state), change);
  }

  public void updateStatusCount(final KsqlQueryStatus state, final int change) {
    this.statuses.compute(state, (key, existing) ->
        existing == null
            ? change
            : existing + change);
  }

  @JsonValue
  public Map<KsqlQueryStatus, Integer> getStatuses() {
    return Collections.unmodifiableMap(statuses);
  }

  public KsqlQueryStatus getAggregateStatus() {
    if (statuses.getOrDefault(KsqlQueryStatus.ERROR, 0) != 0) {
      return KsqlQueryStatus.ERROR;
    }
    return KsqlQueryStatus.RUNNING;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final QueryStatusCount that = (QueryStatusCount) o;
    return Objects.equals(statuses, that.statuses);
  }

  @Override
  public int hashCode() {
    return Objects.hash(statuses);
  }

  @Override
  public String toString() {
    return Joiner.on(",").withKeyValueSeparator(":").join(this.statuses);
  }
  
  private static EnumMap<KsqlQueryStatus, Integer> returnEnumMap() {
    return new EnumMap<>(KsqlQueryStatus.class);
  }
}
