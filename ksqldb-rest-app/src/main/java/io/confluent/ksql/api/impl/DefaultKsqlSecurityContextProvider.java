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

package io.confluent.ksql.api.impl;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.api.auth.ApiSecurityContext;
import io.confluent.ksql.rest.client.KsqlClient;
import io.confluent.ksql.rest.server.services.RestServiceContextFactory.DefaultServiceContextFactory;
import io.confluent.ksql.rest.server.services.RestServiceContextFactory.UserServiceContextFactory;
import io.confluent.ksql.security.KsqlPrincipal;
import io.confluent.ksql.security.KsqlSecurityContext;
import io.confluent.ksql.security.KsqlSecurityExtension;
import io.confluent.ksql.services.ConnectClientFactory;
import io.confluent.ksql.util.KsqlConfig;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.function.Supplier;

public class DefaultKsqlSecurityContextProvider implements KsqlSecurityContextProvider {

  private final KsqlSecurityExtension securityExtension;
  private final DefaultServiceContextFactory defaultServiceContextFactory;
  private final UserServiceContextFactory userServiceContextFactory;
  private final KsqlConfig ksqlConfig;
  private final Supplier<SchemaRegistryClient> schemaRegistryClientFactory;
  private final ConnectClientFactory connectClientFactory;
  private final KsqlClient sharedClient;

  @SuppressFBWarnings(value = "EI_EXPOSE_REP2")
  public DefaultKsqlSecurityContextProvider(
      final KsqlSecurityExtension securityExtension,
      final DefaultServiceContextFactory defaultServiceContextFactory,
      final UserServiceContextFactory userServiceContextFactory,
      final KsqlConfig ksqlConfig,
      final Supplier<SchemaRegistryClient> schemaRegistryClientFactory,
      final ConnectClientFactory connectClientFactory,
      final KsqlClient sharedClient) {
    this.securityExtension = securityExtension;
    this.defaultServiceContextFactory = defaultServiceContextFactory;
    this.userServiceContextFactory = userServiceContextFactory;
    this.ksqlConfig = ksqlConfig;
    this.schemaRegistryClientFactory = schemaRegistryClientFactory;
    this.connectClientFactory = connectClientFactory;
    this.sharedClient = sharedClient;
  }

  @Override
  public KsqlSecurityContext provide(final ApiSecurityContext apiSecurityContext) {

    final Optional<KsqlPrincipal> principal = apiSecurityContext.getPrincipal();
    final Optional<String> authHeader = apiSecurityContext.getAuthHeader();
    final List<Entry<String, String>> requestHeaders = apiSecurityContext.getRequestHeaders();

    // A user context is not necessary if a user context provider is not present or the user
    // principal is missing. If a failed authentication attempt results in a missing principle,
    // then the authentication plugin will have already failed the connection before calling
    // this method. Therefore, if we've reached this method with a missing principle, then this
    // must be a valid connection that does not require authentication.
    // For these cases, we create a default service context that the missing user can use.
    final boolean requiresUserContext =
        securityExtension != null
            && securityExtension.getUserContextProvider().isPresent()
            && principal.isPresent();

    if (!requiresUserContext) {
      return new KsqlSecurityContext(
          principal,
          defaultServiceContextFactory.create(
              ksqlConfig,
              authHeader,
              schemaRegistryClientFactory,
              connectClientFactory,
              sharedClient,
              requestHeaders,
              principal)
      );
    }

    return securityExtension.getUserContextProvider()
        .map(provider -> new KsqlSecurityContext(
            principal,
            userServiceContextFactory.create(
                ksqlConfig,
                authHeader,
                provider.getKafkaClientSupplier(principal.get()),
                provider.getSchemaRegistryClientFactory(principal.get()),
                connectClientFactory,
                sharedClient,
                requestHeaders,
                principal)))
        .get();
  }

  @Override
  public void close() {
    connectClientFactory.close();
  }
}
