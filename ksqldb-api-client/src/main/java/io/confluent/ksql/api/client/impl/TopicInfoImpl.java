/*
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.api.client.impl;

import com.google.common.collect.ImmutableList;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.api.client.TopicInfo;
import java.util.List;
import java.util.Objects;

public class TopicInfoImpl implements TopicInfo {

  private final String name;
  private final int partitions;
  private final ImmutableList<Integer> replicasPerPartition;

  TopicInfoImpl(final String name, final int partitions, final List<Integer> replicasPerPartition) {
    this.name = Objects.requireNonNull(name);
    this.partitions = partitions;
    this.replicasPerPartition = ImmutableList.copyOf(Objects.requireNonNull(replicasPerPartition));
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public int getPartitions() {
    return partitions;
  }

  @Override
  @SuppressFBWarnings(
      value = "EI_EXPOSE_REP",
      justification = "replicasPerPartition is ImmutableList"
  )
  public List<Integer> getReplicasPerPartition() {
    return replicasPerPartition;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final TopicInfoImpl topicInfo = (TopicInfoImpl) o;
    return partitions == topicInfo.partitions
        && name.equals(topicInfo.name)
        && replicasPerPartition.equals(topicInfo.replicasPerPartition);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, partitions, replicasPerPartition);
  }

  @Override
  public String toString() {
    return "TopicInfo{"
        + "name='" + name + '\''
        + ", partitions=" + partitions
        + ", replicasPerPartition=" + replicasPerPartition
        + '}';
  }
}
