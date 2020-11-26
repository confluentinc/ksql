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

package io.confluent.ksql.naming;

import io.confluent.ksql.name.SourceName;
import java.util.Set;

/**
 * Interface topic naming strategies must implement
 */
public interface SourceTopicNamingStrategy {

  /**
   * Determine the Kafka topic, if any, that matches the supplied {@code sourceName}.
   *
   * @param sourceName the source to find the topic of.
   * @param topicNames the set of known / accessible topic names.
   * @return the name of the topic that contains the source's data.
   * @throws io.confluent.ksql.util.KsqlException if the topic name could not be determined.
   */
  String resolveExistingTopic(SourceName sourceName, Set<String> topicNames);
}
