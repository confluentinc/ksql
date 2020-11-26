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
import io.confluent.ksql.properties.with.CommonCreateConfigs;
import io.confluent.ksql.util.KsqlException;
import java.util.Set;

/**
 * Naming strategy that will match existing topic names exactly.
 *
 * <p>Set in {@link io.confluent.ksql.util.KsqlConfig#KSQL_SOURCE_TOPIC_NAMING_STRATEGY_CONFIG}.
 */
public class CaseSensitiveSourceTopicNamingStrategy implements SourceTopicNamingStrategy {

  @Override
  public String resolveExistingTopic(final SourceName sourceName, final Set<String> topicNames) {
    return topicNames.stream()
        .filter(name -> name.equals(sourceName.text()))
        .findFirst()
        .orElseThrow(() -> new KsqlException(
            "No existing topic named " + sourceName + " (case-sensitive)"
                + System.lineSeparator()
                + "You can specify an explicit existing topic name by setting '"
                + CommonCreateConfigs.KAFKA_TOPIC_NAME_PROPERTY + "' in the WITH clause. "
                + "Alternatively, if you intended to create a new topic, set '"
                + CommonCreateConfigs.SOURCE_NUMBER_OF_PARTITIONS + "' in the WITH clause."
        ));
  }
}
