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
import org.apache.kafka.streams.KafkaStreams.State;

/**
 * Used to keep track of a the state of KafkaStreams application
 * across multiple servers. Used in {@link RunningQuery}.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class RawQueryStatusCount {

  private final EnumMap<State, Integer> statuses;

  public RawQueryStatusCount() {
    this(Collections.emptyMap());
  }

  @JsonCreator
  public RawQueryStatusCount(final Map<KafkaStreams.State, Integer> states) {
    this.statuses = states.isEmpty()
        ? new EnumMap<>(KafkaStreams.State.class)
        : new EnumMap<>(states);
  }

  public void updateRawStatusCount(final KafkaStreams.State state, final int change) {
    this.statuses.compute(state, (key, existing) ->
        existing == null
            ? change
            : existing + change);
  }

  @JsonValue
  public Map<KafkaStreams.State, Integer> getStatuses() {
    return Collections.unmodifiableMap(statuses);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final RawQueryStatusCount that = (RawQueryStatusCount) o;
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

}
