/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.services;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.utils.Utils;

/**
 * A topic description to use when trying out operations.
 *
 * <p>It adds the {@code authorizedOperations} variable. The {@code TopicDescription} does not
 * have a public constructor to add this information.
 */
public final class SandboxedTopicDescription extends TopicDescription {
  private final String name;
  private final boolean internal;
  private final List<TopicPartitionInfo> partitions;
  private final Set<AclOperation> authorizedOperations;

  public SandboxedTopicDescription(
      final String name,
      final boolean internal,
      final List<TopicPartitionInfo> partitions,
      final Set<AclOperation> authorizedOperations
  ) {
    super(name, internal, partitions);

    this.name = name;
    this.internal = internal;
    this.partitions = partitions;
    this.authorizedOperations = authorizedOperations;
  }

  @Override
  public Set<AclOperation> authorizedOperations() {
    return this.authorizedOperations;
  }

  @Override
  public boolean equals(final Object o) {
    return super.equals(o)
        && Objects.equals(
            this.authorizedOperations, ((SandboxedTopicDescription)o).authorizedOperations);
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 37 * result + Objects.hash(this.authorizedOperations);
    return result;
  }

  @Override
  public String toString() {
    return "(name=" + this.name + ", internal=" + this.internal + ", partitions="
        + Utils.join(this.partitions, ",") + ", authorizedOperations="
        + this.authorizedOperations + ")";
  }
}
