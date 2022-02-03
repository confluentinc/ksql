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

import io.confluent.ksql.security.DefaultKsqlPrincipal;
import java.nio.charset.StandardCharsets;
import java.security.Principal;
import java.util.Base64;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * Principal implementation created when authenticating with the JaasAuthProvider
 * <p>
 *  This class and its constructor are public to make them accessible from pluggable security
 *  extensions.
 * </p>
 */
public class JaasPrincipal extends DefaultKsqlPrincipal {

  private final String name;
  private final String password;
  private final String token;

  public JaasPrincipal(final String name, final String password) {
    this(name, password, "");
  }

  private JaasPrincipal(final String name, final String password, final String ipAddress) {
    super(new BasicJaasPrincipal(name), ipAddress);

    this.name = Objects.requireNonNull(name, "name");
    this.password = Objects.requireNonNull(password, "password");
    this.token = createToken(name, password);
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public Map<String, Object> getUserProperties() {
    return Collections.emptyMap();
  }

  public String getToken() {
    return token;
  }

  /**
   * Preserve token functionality by returning another JaasPrincipal when the
   * IP address is set from the routing context.
   */
  @Override
  public DefaultKsqlPrincipal withIpAddress(final String ipAddress) {
    return new JaasPrincipal(name, password, ipAddress);
  }

  private static String createToken(final String name, final String secret) {
    return Base64.getEncoder().encodeToString((name + ":" + secret)
        .getBytes(StandardCharsets.ISO_8859_1));
  }

  static class BasicJaasPrincipal implements Principal {

    private final String name;

    BasicJaasPrincipal(final String name) {
      this.name = name;
    }

    @Override
    public String getName() {
      return name;
    }
  }
}

