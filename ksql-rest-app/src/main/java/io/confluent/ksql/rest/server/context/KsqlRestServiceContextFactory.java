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

import static java.util.Objects.requireNonNull;

import com.google.common.annotations.VisibleForTesting;
import io.confluent.ksql.rest.server.services.RestServiceContextFactory;
import io.confluent.ksql.rest.server.services.RestServiceContextFactory.DefaultServiceContextFactory;
import io.confluent.ksql.rest.server.services.RestServiceContextFactory.UserServiceContextFactory;
import io.confluent.ksql.security.KsqlSecurityExtension;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlConfig;
import java.security.Principal;
import java.util.Optional;
import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.HttpHeaders;
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
    KsqlRestServiceContextFactory.ksqlConfig = requireNonNull(ksqlConfig, "ksqlConfig");
    KsqlRestServiceContextFactory.securityExtension
        = requireNonNull(securityExtension, "securityExtension");
  }

  private final SecurityContext securityContext;
  private final DefaultServiceContextFactory defaultServiceContextFactory;
  private final UserServiceContextFactory userServiceContextFactory;
  private final HttpServletRequest request;

  @Inject
  public KsqlRestServiceContextFactory(
      final SecurityContext securityContext,
      final HttpServletRequest request
  ) {
    this(
        securityContext,
        request,
        RestServiceContextFactory::create,
        RestServiceContextFactory::create
    );
  }

  @VisibleForTesting
  KsqlRestServiceContextFactory(
      final SecurityContext securityContext,
      final HttpServletRequest request,
      final DefaultServiceContextFactory defaultServiceContextFactory,
      final UserServiceContextFactory userServiceContextFactory
  ) {
    this.securityContext = requireNonNull(securityContext, "securityContext");
    this.defaultServiceContextFactory = requireNonNull(defaultServiceContextFactory,
        "defaultServiceContextFactory");
    this.userServiceContextFactory = requireNonNull(userServiceContextFactory,
        "userServiceContextFactory");
    this.request = requireNonNull(request, "request");
  }

  @Override
  public ServiceContext provide() {
    final Optional<String> authHeader =
        Optional.ofNullable(request.getHeader(HttpHeaders.AUTHORIZATION));

    if (!securityExtension.getUserContextProvider().isPresent()) {
      return defaultServiceContextFactory.create(ksqlConfig, authHeader);
    }

    final Principal principal = securityContext.getUserPrincipal();
    return securityExtension.getUserContextProvider()
        .map(provider ->
            userServiceContextFactory.create(
                ksqlConfig,
                authHeader,
                provider.getKafkaClientSupplier(principal),
                provider.getSchemaRegistryClientFactory(principal)))
        .get();
  }

  @Override
  public void dispose(final ServiceContext serviceContext) {
    serviceContext.close();
  }
}
