package io.confluent.ksql.rest.entity;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.Immutable;
import java.util.Map;
import java.util.Objects;

/**
 * Represents a the lags associated with a particular state store on a particular host.
 */
@Immutable
@JsonIgnoreProperties(ignoreUnknown = true)
public class StateStoreLags {

  private final ImmutableMap<Integer, LagInfoEntity> lagByPartition;

  @JsonCreator
  public StateStoreLags(
      @JsonProperty("lagByPartition") final Map<Integer, LagInfoEntity> lagByPartition) {
    this.lagByPartition = ImmutableMap.copyOf(requireNonNull(lagByPartition, "lagByPartition"));
  }

  public LagInfoEntity getLagByPartition(int partition) {
    return lagByPartition.get(partition);
  }

  public Map<Integer, LagInfoEntity> getLagByPartition() {
    return lagByPartition;
  }

  public int getSize() {
    return lagByPartition.size();
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final StateStoreLags that = (StateStoreLags) o;
    return Objects.equals(lagByPartition, that.lagByPartition);
  }

  @Override
  public int hashCode() {
    return Objects.hash(lagByPartition);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("lagByPartition", lagByPartition)
        .toString();
  }
}
