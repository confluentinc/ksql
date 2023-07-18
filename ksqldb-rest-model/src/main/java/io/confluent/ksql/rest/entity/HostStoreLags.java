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

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.Immutable;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

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

  public Optional<StateStoreLags> getStateStoreLags(final QueryStateStoreId queryStateStoreId) {
    return Optional.ofNullable(stateStoreLags.get(queryStateStoreId));
  }

  @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "stateStoreLags is ImmutableMap")
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
