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

import io.confluent.ksql.api.client.TopicInfo;
import java.util.List;
import java.util.Objects;

public class TopicInfoImpl implements TopicInfo {

  private final String name;
  private final int partitions;
  private final List<Integer> replicasPerPartition;

  TopicInfoImpl(final String name, final int partitions, final List<Integer> replicasPerPartition) {
    this.name = Objects.requireNonNull(name);
    this.partitions = partitions;
    this.replicasPerPartition = Objects.requireNonNull(replicasPerPartition);
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
  public List<Integer> getReplicasPerPartition() {
    return replicasPerPartition;
  }
}
