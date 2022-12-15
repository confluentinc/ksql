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

import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;
import com.google.errorprone.annotations.Immutable;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Objects;
import java.util.Set;

@Immutable
@JsonIgnoreProperties(ignoreUnknown = true)
public class ActiveStandbyEntity {

  private final ImmutableSet<String> activeStores;
  private final ImmutableSet<TopicPartitionEntity> activePartitions;
  private final ImmutableSet<String> standByStores;
  private final ImmutableSet<TopicPartitionEntity> standByPartitions;

  @JsonCreator
  public ActiveStandbyEntity(
      @JsonProperty("activeStores") final Set<String> activeStores,
      @JsonProperty("activePartitions") final Set<TopicPartitionEntity> activePartitions,
      @JsonProperty("standByStores") final Set<String> standByStores,
      @JsonProperty("standByPartitions") final Set<TopicPartitionEntity> standByPartitions
  ) {
    this.activeStores = ImmutableSet.copyOf(requireNonNull(activeStores));
    this.activePartitions = ImmutableSet.copyOf(requireNonNull(activePartitions));
    this.standByStores = ImmutableSet.copyOf(requireNonNull(standByStores));
    this.standByPartitions = ImmutableSet.copyOf(requireNonNull(standByPartitions));
  }

  @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "activeStores is ImmutableSet")
  public Set<String> getActiveStores() {
    return activeStores;
  }

  @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "standByStores is ImmutableSet")
  public Set<String> getStandByStores() {
    return standByStores;
  }

  @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "activePartitions is ImmutableSet")
  public Set<TopicPartitionEntity> getActivePartitions() {
    return activePartitions;
  }

  @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "standByPartitions is ImmutableSet")
  public Set<TopicPartitionEntity> getStandByPartitions() {
    return standByPartitions;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final ActiveStandbyEntity that = (ActiveStandbyEntity) o;
    return Objects.equals(activeStores, that.activeStores)
        && Objects.equals(standByStores, that.standByStores)
        && Objects.equals(activePartitions, that.activePartitions)
        && Objects.equals(standByPartitions, that.standByPartitions);
  }

  @Override
  public int hashCode() {
    return Objects.hash(activeStores, standByStores, activePartitions, standByPartitions);
  }

  @Override
  public String toString() {
    return "ActiveStandbyEntity{"
        + " Active stores = " + activeStores
        + ", Active partitions = " + activePartitions
        + ", Standby stores = " + standByStores
        + ", Standby partitions = " + standByPartitions
        + "}";
  }
}
