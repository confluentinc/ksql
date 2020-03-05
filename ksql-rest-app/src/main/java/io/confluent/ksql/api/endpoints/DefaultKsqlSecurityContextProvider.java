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

package io.confluent.ksql.api.endpoints;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.api.server.ApiSecurityContext;
import io.confluent.ksql.rest.server.services.RestServiceContextFactory.DefaultServiceContextFactory;
import io.confluent.ksql.rest.server.services.RestServiceContextFactory.UserServiceContextFactory;
import io.confluent.ksql.security.KsqlSecurityContext;
import io.confluent.ksql.security.KsqlSecurityExtension;
import io.confluent.ksql.util.KsqlConfig;
import java.security.Principal;
import java.util.Optional;
import java.util.function.Supplier;

public class DefaultKsqlSecurityContextProvider implements KsqlSecurityContextProvider {

  private final KsqlSecurityExtension securityExtension;
  private final DefaultServiceContextFactory defaultServiceContextFactory;
  private final UserServiceContextFactory userServiceContextFactory;
  private final KsqlConfig ksqlConfig;
  private final Supplier<SchemaRegistryClient> schemaRegistryClientFactory;

  public DefaultKsqlSecurityContextProvider(
      final KsqlSecurityExtension securityExtension,
      final DefaultServiceContextFactory defaultServiceContextFactory,
      final UserServiceContextFactory userServiceContextFactory,
      final KsqlConfig ksqlConfig,
      final Supplier<SchemaRegistryClient> schemaRegistryClientFactory) {
    this.securityExtension = securityExtension;
    this.defaultServiceContextFactory = defaultServiceContextFactory;
    this.userServiceContextFactory = userServiceContextFactory;
    this.ksqlConfig = ksqlConfig;
    this.schemaRegistryClientFactory = schemaRegistryClientFactory;
  }

  @Override
  public KsqlSecurityContext provide(final ApiSecurityContext apiSecurityContext) {

    final Principal principal = apiSecurityContext.getPrincipal();
    final Optional<String> authHeader = apiSecurityContext.getAuthToken();

    if (securityExtension == null || !securityExtension.getUserContextProvider().isPresent()) {
      return new KsqlSecurityContext(
          Optional.ofNullable(principal),
          defaultServiceContextFactory.create(ksqlConfig, authHeader, schemaRegistryClientFactory)
      );
    }

    return securityExtension.getUserContextProvider()
        .map(provider -> new KsqlSecurityContext(
            Optional.ofNullable(principal),
            userServiceContextFactory.create(
                ksqlConfig,
                authHeader,
                provider.getKafkaClientSupplier(principal),
                provider.getSchemaRegistryClientFactory(principal))))
        .get();
  }

}
