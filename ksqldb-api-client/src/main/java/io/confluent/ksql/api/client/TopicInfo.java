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

package io.confluent.ksql.api.client;

import java.util.List;

/**
 * Metadata for a Kafka topic available for use with ksqlDB.
 */
public interface TopicInfo {

  /**
   * @return the name of this topic
   */
  String getName();

  /**
   * @return the number of partitions for this topic
   */
  int getPartitions();

  /**
   * Returns the number of replicas for each topic partition.
   *
   * @return a list with size equal to the number of partitions. Each element is the number of
   *         replicas for the partition corresponding to the list index.
   */
  List<Integer> getReplicasPerPartition();

}
