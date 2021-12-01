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

import java.nio.charset.StandardCharsets;
import java.security.Principal;
import java.util.Base64;
import java.util.Objects;

/**
 * Principal implementation created when authenticating with the JaasAuthProvider
 */
public class JaasPrincipal implements Principal {

  private final String name;

  private final String token;

  public JaasPrincipal(final String name, final String password) {
    this.name = Objects.requireNonNull(name);
    this.token = createToken(name, Objects.requireNonNull(password));
  }

  private String createToken(final String name, final String secret) {
    return Base64.getEncoder().encodeToString((name + ":" + secret)
        .getBytes(StandardCharsets.ISO_8859_1));
  }

  public String getToken() {
    return token;
  }

  @Override
  public String getName() {
    return name;
  }
}

