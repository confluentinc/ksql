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

import io.confluent.ksql.services.KafkaClusterUtil;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlServerException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class TopicAccessValidatorFactory {
  private static final Logger LOG = LoggerFactory.getLogger(TopicAccessValidatorFactory.class);
  private static final String KAFKA_AUTHORIZER_CLASS_NAME = "authorizer.class.name";
  private static final TopicAccessValidator DUMMY_VALIDATOR = (sc, metastore, statement) -> { };

  private TopicAccessValidatorFactory() {
  }

  public static TopicAccessValidator create(
      final KsqlConfig ksqlConfig,
      final ServiceContext serviceContext
  ) {
    final String enabled = ksqlConfig.getString(KsqlConfig.KSQL_ENABLE_TOPIC_ACCESS_VALIDATOR);
    if (enabled.equals(KsqlConfig.KSQL_ACCESS_VALIDATOR_ON)) {
      LOG.info("Forcing topic access validator");
      return new AuthorizationTopicAccessValidator();
    } else if (enabled.equals(KsqlConfig.KSQL_ACCESS_VALIDATOR_OFF)) {
      return DUMMY_VALIDATOR;
    }

    final AdminClient adminClient = serviceContext.getAdminClient();

    if (isKafkaAuthorizerEnabled(adminClient)) {
      if (KafkaClusterUtil.isAuthorizedOperationsSupported(adminClient)) {
        LOG.info("KSQL topic authorization checks enabled.");
        return new AuthorizationTopicAccessValidator();
      }

      LOG.warn("The Kafka broker has an authorization service enabled, but the Kafka "
          + "version does not support authorizedOperations(). "
          + "KSQL topic authorization checks will not be enabled.");
    }
    return DUMMY_VALIDATOR;
  }

  private static boolean isKafkaAuthorizerEnabled(final AdminClient adminClient) {
    try {
      final ConfigEntry configEntry =
          KafkaClusterUtil.getConfig(adminClient).get(KAFKA_AUTHORIZER_CLASS_NAME);

      return configEntry != null
          && configEntry.value() != null
          && !configEntry.value().isEmpty();
    } catch (final KsqlServerException e) {
      // If ClusterAuthorizationException is thrown is because the AdminClient is not authorized
      // to describe cluster configs. This also means authorization is enabled.
      if (e.getCause() instanceof ClusterAuthorizationException) {
        return true;
      }

      // Throw the unknown exception to avoid leaving the Server unsecured if a different
      // error was thrown
      throw e;
    }
  }
}
