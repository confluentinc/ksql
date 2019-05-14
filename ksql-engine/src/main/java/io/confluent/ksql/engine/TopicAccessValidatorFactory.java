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

package io.confluent.ksql.engine;

import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.util.KsqlConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class TopicAccessValidatorFactory {
  private static final Logger LOG = LoggerFactory.getLogger(TopicAccessValidatorFactory.class);

  private TopicAccessValidatorFactory() {

  }

  public static TopicAccessValidator create(
      final KsqlConfig ksqlConfig,
      final MetaStore metaStore
  ) {
    if (ksqlConfig.getBoolean(KsqlConfig.KSQL_TOPIC_AUTHORIZATION_ENABLED)) {
      LOG.info("KSQL topic authorization checks enabled.");
      return new AuthorizationTopicAccessValidator(metaStore);
    }

    // Dummy validator if a Kafka authorizer is not enabled
    return (sc, statement) -> {
      return;
    };
  }
}
