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
 * Represents a host's lag information, and when the lag information was collected.
 */
@Immutable
@JsonIgnoreProperties(ignoreUnknown = true)
public class HostStoreLags {

  private final ImmutableMap<QueryStateStoreId, StateStoreLags> stateStoreLags;
  private final long updateTimeMs;

  @JsonCreator
  public HostStoreLags(
      @JsonProperty("stateStoreLags") final Map<QueryStateStoreId, StateStoreLags> stateStoreLags,
      @JsonProperty("updateTimeMs") final long updateTimeMs) {
    this.stateStoreLags = ImmutableMap.copyOf(requireNonNull(stateStoreLags, "stateStoreLags"));
    this.updateTimeMs = updateTimeMs;
  }

  public StateStoreLags getStateStoreLags(final QueryStateStoreId queryStateStoreId) {
    return stateStoreLags.get(queryStateStoreId);
  }

  public StateStoreLags getStateStoreLagsOrDefault(final QueryStateStoreId queryStateStoreId,
                                                   final StateStoreLags defaultStateStoreLags) {
    return stateStoreLags.getOrDefault(queryStateStoreId, defaultStateStoreLags);
  }

  public Map<QueryStateStoreId, StateStoreLags> getStateStoreLags() {
    return stateStoreLags;
  }

  public long getUpdateTimeMs() {
    return updateTimeMs;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final HostStoreLags that = (HostStoreLags) o;
    return Objects.equals(stateStoreLags, that.stateStoreLags)
        && updateTimeMs == that.updateTimeMs;
  }

  @Override
  public int hashCode() {
    return Objects.hash(stateStoreLags, updateTimeMs);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("stateStoreLags", stateStoreLags)
        .add("updateTimeMs", updateTimeMs)
        .toString();
  }
}
