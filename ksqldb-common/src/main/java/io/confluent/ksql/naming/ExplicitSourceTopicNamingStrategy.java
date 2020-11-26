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
 * Naming strategy that requires the user to provide the name of the topic in the WITH clause.
 *
 * <p>If the WITH clause is missing the KAFKA_TOPIC, then this strategy will through an exception
 * informing the user to add it.
 *
 * <p>Set in {@link io.confluent.ksql.util.KsqlConfig#KSQL_SOURCE_TOPIC_NAMING_STRATEGY_CONFIG}.
 */
@SuppressWarnings("unused") // Invoked via reflection
public class ExplicitSourceTopicNamingStrategy implements SourceTopicNamingStrategy {

  @Override
  public String resolveExistingTopic(final SourceName sourceName, final Set<String> topicNames) {
    throw new KsqlException("Explicit '"
        + CommonCreateConfigs.KAFKA_TOPIC_NAME_PROPERTY
        + "' required in the WITH clause");
  }
}