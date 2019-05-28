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
import io.confluent.ksql.services.KafkaClusterUtil;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlServerException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class TopicAccessValidatorFactory {
  private static final Logger LOG = LoggerFactory.getLogger(TopicAccessValidatorFactory.class);
  private static final String KAFKA_AUTHORIZER_CLASS_NAME = "authorizer.class.name";

  private TopicAccessValidatorFactory() {

  }

  public static TopicAccessValidator create(
      final ServiceContext serviceContext,
      final MetaStore metaStore
  ) {
    final AdminClient adminClient = serviceContext.getAdminClient();

    if (isKafkaAuthorizerEnabled(adminClient)) {
      if (KafkaClusterUtil.isAuthorizedOperationsSupported(adminClient)) {
        LOG.info("KSQL topic authorization checks enabled.");
        return new AuthorizationTopicAccessValidator();
      }

      LOG.info("The Kafka broker has an authorization service enabled, but the Kafka "
          + "version does not support authorizedOperations(). "
          + "KSQL topic authorization checks will not be enabled.");
    }

    // Dummy validator if a Kafka authorizer is not enabled
    return (sc, metastore, statement) -> {
      return;
    };
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
