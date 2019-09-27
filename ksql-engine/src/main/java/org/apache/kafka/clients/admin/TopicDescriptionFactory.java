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

package org.apache.kafka.clients.admin;

import java.util.List;
import java.util.Set;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.acl.AclOperation;

/**
 * Factory to allow access to package private constructor KSQL needs
 */
public final class TopicDescriptionFactory {

  private TopicDescriptionFactory() {
  }

  public static TopicDescription create(
      final String name,
      final boolean internal,
      final List<TopicPartitionInfo> partitions,
      final Set<AclOperation> authorizedOperations
  ) {
    return new TopicDescription(name, internal, partitions, authorizedOperations);
  }
}
