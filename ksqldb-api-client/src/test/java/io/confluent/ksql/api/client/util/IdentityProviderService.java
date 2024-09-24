/*
 * Copyright 2024 Confluent Inc.
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

package io.confluent.ksql.api.client.util;

import java.time.Duration;

import dasniko.testcontainers.keycloak.KeycloakContainer;

public class IdentityProviderService {

  // Json file containing keycloak realm config.
  // Keycloak testcontainer is initialized with this config
  // containing clients for tests requiring client credentials flow.
  private static final String DEFAULT_REALM_CONFIG_PATH = "/IntegrationTestsRealm.json";

  private static final String ISSUER_FORMAT = "/realms/IntegrationTestsRealm";

  private static final String JWKS_ENDPOINT_FORMAT =
      "/realms/IntegrationTestsRealm/protocol/openid-connect/certs";

  private static final String TOKEN_ENDPOINT_FORMAT =
      "/realms/IntegrationTestsRealm/protocol/openid-connect/token";

  private final KeycloakContainer keycloak;

  public IdentityProviderService() {
    this(DEFAULT_REALM_CONFIG_PATH);
  }

  public IdentityProviderService(String realmConfigPath) {
    this.keycloak = new KeycloakContainer()
        .withRealmImportFile(realmConfigPath);
  }

  public void start() {
    this.keycloak.start();
  }

  public String getJwksEndpoint() {
    return this.keycloak.getAuthServerUrl() + JWKS_ENDPOINT_FORMAT;
  }

  public String getTokenEndpoint() {
    return this.keycloak.getAuthServerUrl() + TOKEN_ENDPOINT_FORMAT;
  }

  public String getIssuer() {
    return this.keycloak.getAuthServerUrl() + ISSUER_FORMAT;
  }

  public void shutdown() {
    this.keycloak.stop();
  }

  public void setStartupTimeout(Duration duration) {
    this.keycloak.withStartupTimeout(duration);
  }

  public void setCommand(String cmd) {
    this.keycloak.withCommand(cmd);
  }
}
