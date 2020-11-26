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

import com.google.common.collect.Iterables;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.properties.with.CommonCreateConfigs;
import io.confluent.ksql.util.GrammaticalJoiner;
import io.confluent.ksql.util.KsqlException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Naming strategy that will match existing topic names case-insensitively.
 *
 * <p>Where multiple matches occur, any exact match wins, or else an exception is thrown.
 *
 * <p>Set in {@link io.confluent.ksql.util.KsqlConfig#KSQL_SOURCE_TOPIC_NAMING_STRATEGY_CONFIG}.
 */
public class CaseInsensitiveSourceTopicNamingStrategy implements SourceTopicNamingStrategy {

  @Override
  public String resolveExistingTopic(final SourceName sourceName, final Set<String> topicNames) {
    final List<String> candidates = new ArrayList<>();
    for (final String topicName : topicNames) {
      if (!sourceName.text().equalsIgnoreCase(topicName)) {
        continue;
      }

      if (sourceName.text().equals(topicName)) {
        // Exact match!
        return topicName;
      }

      candidates.add(topicName);
    }

    if (candidates.isEmpty()) {
      throw new KsqlException("No existing topic named " + sourceName + " (case-insensitive)"
          + System.lineSeparator()
          + "You can specify an explicit existing topic name by setting '"
          + CommonCreateConfigs.KAFKA_TOPIC_NAME_PROPERTY + "' in the WITH clause. "
          + "Alternatively, if you intended to create a new topic, set '"
          + CommonCreateConfigs.SOURCE_NUMBER_OF_PARTITIONS + "' in the WITH clause."
      );
    }

    if (candidates.size() > 1) {
      throw new KsqlException("Multiple existing topics found that match "
          + sourceName + " (case-insensitive)."
          + System.lineSeparator()
          + "Add an explicit '" + CommonCreateConfigs.KAFKA_TOPIC_NAME_PROPERTY
          + "' property to the WITH clause to choose either "
          + GrammaticalJoiner.or().join(candidates)
      );
    }

    return Iterables.getOnlyElement(candidates);
  }
}
