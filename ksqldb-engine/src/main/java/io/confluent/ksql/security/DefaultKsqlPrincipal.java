/*
 * Copyright 2021 Confluent Inc.
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

import java.security.Principal;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * A {@link KsqlPrincipal} implementation that wraps another {@code Principal}.
 * If the wrapped principal is a KsqlPrincipal, then its user properties are
 * passed through. Otherwise, an empty map is returned.
 */
public class DefaultKsqlPrincipal implements KsqlPrincipal {

  private final Principal principal;
  private final String ipAddress;
  private final int port;

  public DefaultKsqlPrincipal(final Principal principal) {
    this(principal, "", 0);
  }

  protected DefaultKsqlPrincipal(final Principal principal,
      final String ipAddress, final int port) {
    this.principal = Objects.requireNonNull(principal, "principal");
    this.ipAddress = Objects.requireNonNull(ipAddress, "ipAddress");
    this.port = port;
  }

  @Override
  public String getName() {
    return principal.getName();
  }

  @Override
  public Map<String, Object> getUserProperties() {
    if (principal instanceof KsqlPrincipal) {
      return ((KsqlPrincipal) principal).getUserProperties();
    } else {
      return Collections.emptyMap();
    }
  }

  /**
   * Exposes the wrapped principal so custom extensions can access the original
   * principal. This is part of the public API and should not be removed.
   */
  public Principal getOriginalPrincipal() {
    return principal;
  }

  @Override
  public String getIpAddress() {
    return ipAddress;
  }

  @Override
  public int getPort() {
    return port;
  }

  /**
   * IP address and port are populated from the request context, and subsequently passed
   * throughout the engine.
   */
  public DefaultKsqlPrincipal withIpAddressAndPort(final String ipAddress, final int port) {
    return new DefaultKsqlPrincipal(principal, ipAddress, port);
  }
}
