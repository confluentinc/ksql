/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.rest.server.context;

import io.confluent.ksql.rest.server.security.KsqlSecurityExtension;
import io.confluent.ksql.services.DefaultServiceContext;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlConfig;
import java.security.Principal;
import java.util.Objects;
import javax.inject.Inject;
import javax.ws.rs.core.SecurityContext;
import org.glassfish.hk2.api.Factory;

/**
 * This class implements {@link Factory}, which allows a REST application to create
 * a new {@link ServiceContext} during REST requests.
 */
public class KsqlRestServiceContextFactory implements Factory<ServiceContext> {
  private static KsqlConfig ksqlConfig;
  private static KsqlSecurityExtension securityExtension;

  public static void configure(
      final KsqlConfig ksqlConfig,
      final KsqlSecurityExtension securityExtension
  ) {
    KsqlRestServiceContextFactory.ksqlConfig = Objects.requireNonNull(ksqlConfig, "ksqlConfig");
    KsqlRestServiceContextFactory.securityExtension
        = Objects.requireNonNull(securityExtension, "securityExtension");
  }

  private final SecurityContext securityContext;

  @Inject
  public KsqlRestServiceContextFactory(final SecurityContext securityContext) {
    this.securityContext = securityContext;
  }

  @Override
  public ServiceContext provide() {
    if (!securityExtension.getAuthorizationProvider().isPresent()) {
      return DefaultServiceContext.create(ksqlConfig);
    }

    final Principal principal = securityContext.getUserPrincipal();
    return securityExtension.getUserContextProvider()
        .map(provider ->
            DefaultServiceContext.create(
                ksqlConfig,
                provider.getKafkaClientSupplier(principal),
                provider.getSchemaRegistryClientFactory(principal)))
        .get();
  }

  @Override
  public void dispose(final ServiceContext serviceContext) {
    serviceContext.close();
  }
}
