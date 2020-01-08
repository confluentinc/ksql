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
import io.confluent.ksql.rest.server.services.RestServiceContextFactory.UserServiceContextFactory;
import io.confluent.ksql.security.KsqlSecurityContext;
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
 * a new {@link KsqlSecurityContext} during REST requests.
 */
public class KsqlSecurityContextBinderFactory implements Factory<KsqlSecurityContext> {
  private static KsqlConfig ksqlConfig;
  private static KsqlSecurityExtension securityExtension;
  private static ServiceContext defaultServiceContext;

  public static void configure(
      final KsqlConfig ksqlConfig,
      final KsqlSecurityExtension securityExtension,
      final ServiceContext defaultServiceContext
  ) {
    KsqlSecurityContextBinderFactory.ksqlConfig = requireNonNull(ksqlConfig, "ksqlConfig");
    KsqlSecurityContextBinderFactory.securityExtension
        = requireNonNull(securityExtension, "securityExtension");
    KsqlSecurityContextBinderFactory.defaultServiceContext
        = requireNonNull(defaultServiceContext, "defaultServiceContext");
  }

  private final SecurityContext securityContext;
  private final UserServiceContextFactory userServiceContextFactory;
  private final HttpServletRequest request;

  @Inject
  public KsqlSecurityContextBinderFactory(
      final SecurityContext securityContext,
      final HttpServletRequest request
  ) {
    this(
        securityContext,
        request,
        RestServiceContextFactory::create
    );
  }

  @VisibleForTesting
  KsqlSecurityContextBinderFactory(
      final SecurityContext securityContext,
      final HttpServletRequest request,
      final UserServiceContextFactory userServiceContextFactory
  ) {
    this.securityContext = requireNonNull(securityContext, "securityContext");
    this.userServiceContextFactory = requireNonNull(userServiceContextFactory,
        "userServiceContextFactory");
    this.request = requireNonNull(request, "request");
  }

  @Override
  public KsqlSecurityContext provide() {
    final Principal principal = securityContext.getUserPrincipal();
    final Optional<String> authHeader =
        Optional.ofNullable(request.getHeader(HttpHeaders.AUTHORIZATION));

    if (!securityExtension.getUserContextProvider().isPresent()) {
      return new KsqlSecurityContext(Optional.ofNullable(principal), defaultServiceContext);
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

  @Override
  public void dispose(final KsqlSecurityContext ksqlSecurityContext) {
  }
}
