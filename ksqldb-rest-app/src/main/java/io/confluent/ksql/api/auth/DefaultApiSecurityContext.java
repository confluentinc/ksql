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

package io.confluent.ksql.api.auth;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.api.server.Server;
import io.confluent.ksql.security.KsqlPrincipal;
import io.vertx.ext.auth.User;
import io.vertx.ext.web.RoutingContext;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;

public final class DefaultApiSecurityContext implements ApiSecurityContext {

  private final Optional<KsqlPrincipal> principal;
  private final Optional<String> authToken;
  private final List<Entry<String, String>> requestHeaders;

  public static DefaultApiSecurityContext create(final RoutingContext routingContext,
      final Server server) {
    final User user = routingContext.user();
    if (user != null && !(user instanceof ApiUser)) {
      throw new IllegalStateException("Not an ApiUser: " + user);
    }
    final ApiUser apiUser = (ApiUser) user;

    String authToken = routingContext.request().getHeader("Authorization");
    if (server.getAuthenticationPlugin().isPresent()) {
      authToken = server.getAuthenticationPlugin().get().getAuthToken(routingContext);
    }
    final List<Entry<String, String>> requestHeaders = routingContext.request().headers().entries();
    final String ipAddress = routingContext.request().remoteAddress().host();
    return new DefaultApiSecurityContext(
        apiUser != null
            ? apiUser.getPrincipal().withIpAddress(ipAddress == null ? "" : ipAddress)
            : null,
        authToken,
        requestHeaders);
  }

  private DefaultApiSecurityContext(
      final KsqlPrincipal principal,
      final String authToken,
      final List<Entry<String, String>> requestHeaders) {
    this.principal = Optional.ofNullable(principal);
    this.authToken = Optional.ofNullable(authToken);
    this.requestHeaders = requestHeaders;
  }

  @Override
  public Optional<KsqlPrincipal> getPrincipal() {
    return principal;
  }

  @Override
  public Optional<String> getAuthToken() {
    return authToken;
  }

  @Override
  public List<Entry<String, String>> getRequestHeaders() {
    return ImmutableList.copyOf(requestHeaders);
  }
}
