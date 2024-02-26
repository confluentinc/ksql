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

package io.confluent.ksql.security;

import io.confluent.ksql.services.KafkaClusterUtil;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlServerException;
import java.util.Optional;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class KsqlAuthorizationValidatorFactory {
  private static final Logger LOG = LoggerFactory
      .getLogger(KsqlAuthorizationValidatorFactory.class);
  private static final String KAFKA_AUTHORIZER_CLASS_NAME = "authorizer.class.name";

  private KsqlAuthorizationValidatorFactory() {
  }

  public static Optional<KsqlAuthorizationValidator> create(
      final KsqlConfig ksqlConfig,
      final ServiceContext serviceContext,
      final Optional<KsqlAuthorizationProvider> externalAuthorizationProvider
  ) {
    final Optional<KsqlAccessValidator> accessValidator = getAccessValidator(
        ksqlConfig,
        serviceContext,
        externalAuthorizationProvider
    );

    return accessValidator.map(v ->
        new KsqlAuthorizationValidatorImpl(cacheIfEnabled(ksqlConfig, v)));
  }

  private static Optional<KsqlAccessValidator> getAccessValidator(
      final KsqlConfig ksqlConfig,
      final ServiceContext serviceContext,
      final Optional<KsqlAuthorizationProvider> externalAuthorizationProvider
  ) {
    final String featureFlag = ksqlConfig.getString(KsqlConfig.KSQL_ENABLE_ACCESS_VALIDATOR);
    if (featureFlag.equalsIgnoreCase(KsqlConfig.KSQL_ACCESS_VALIDATOR_OFF)) {
      return Optional.empty();
    }

    if (externalAuthorizationProvider.isPresent()) {
      return Optional.of(new KsqlProvidedAccessValidator(externalAuthorizationProvider.get()));
    } else if (isTopicAccessValidatorEnabled(ksqlConfig, serviceContext)) {
      return Optional.of(new KsqlBackendAccessValidator());
    }

    return Optional.empty();
  }

  private static KsqlAccessValidator cacheIfEnabled(
      final KsqlConfig ksqlConfig,
      final KsqlAccessValidator accessValidator
  ) {
    return isCacheEnabled(ksqlConfig)
        ? new KsqlCacheAccessValidator(ksqlConfig, accessValidator)
        : accessValidator;
  }

  private static boolean isCacheEnabled(final KsqlConfig ksqlConfig) {
    // The cache expiry time is used to decided whether to enable the cache or not
    return ksqlConfig.getLong(KsqlConfig.KSQL_AUTH_CACHE_EXPIRY_TIME_SECS) > 0;
  }

  private static boolean isTopicAccessValidatorEnabled(
      final KsqlConfig ksqlConfig,
      final ServiceContext serviceContext
  ) {
    final String enabled = ksqlConfig.getString(KsqlConfig.KSQL_ENABLE_ACCESS_VALIDATOR);
    if (enabled.equals(KsqlConfig.KSQL_ACCESS_VALIDATOR_ON)) {
      return true;
    }

    // If KSQL_ACCESS_VALIDATOR_AUTO, then check if Kafka has an authorizer enabled
    final Admin adminClient = serviceContext.getAdminClient();
    if (isKafkaAuthorizerEnabled(adminClient)) {
      if (KafkaClusterUtil.isAuthorizedOperationsSupported(adminClient)) {
        LOG.info("KSQL topic authorization checks enabled.");
        return true;
      }

      LOG.warn("The Kafka broker has an authorization service enabled, but the Kafka "
          + "version does not support authorizedOperations(). "
          + "KSQL topic authorization checks will not be enabled.");
    }

    return false;
  }

  private static boolean isKafkaAuthorizerEnabled(final Admin adminClient) {
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
