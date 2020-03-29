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

package io.confluent.ksql.api.client.impl;

import io.confluent.ksql.api.client.ClientOptions;
import java.util.Objects;

public class ClientOptionsImpl implements ClientOptions {

  private String host = "localhost";
  private int port = 8088;
  private boolean useTls = false;
  private boolean useClientAuth = false;
  private boolean useBasicAuth = false;
  private String trustStorePath = "";
  private String trustStorePassword = "";
  private String keyStorePath = "";
  private String keyStorePassword = "";
  private String basicAuthUsername = "";
  private String basicAuthPassword = "";

  public ClientOptionsImpl() {
  }

  // CHECKSTYLE_RULES.OFF: ParameterNumberCheck
  private ClientOptionsImpl(
      // CHECKSTYLE_RULES.ON: ParameterNumberCheck
      final String host, final int port,
      final boolean useTls, final boolean useClientAuth, final boolean useBasicAuth,
      final String trustStorePath, final String trustStorePassword,
      final String keyStorePath, final String keyStorePassword,
      final String basicAuthUsername, final String basicAuthPassword) {
    this.host = Objects.requireNonNull(host);
    this.port = port;
    this.useTls = useTls;
    this.useClientAuth = useClientAuth;
    this.useBasicAuth = useBasicAuth;
    this.trustStorePath = Objects.requireNonNull(trustStorePath);
    this.trustStorePassword = Objects.requireNonNull(trustStorePassword);
    this.keyStorePath = Objects.requireNonNull(keyStorePath);
    this.keyStorePassword = Objects.requireNonNull(keyStorePassword);
    this.basicAuthUsername = Objects.requireNonNull(basicAuthUsername);
    this.basicAuthPassword = Objects.requireNonNull(basicAuthPassword);
  }

  @Override
  public ClientOptions setHost(final String host) {
    this.host = Objects.requireNonNull(host);
    return this;
  }

  @Override
  public ClientOptions setPort(final int port) {
    this.port = port;
    return this;
  }

  @Override
  public ClientOptions setUseTls(final boolean useTls) {
    this.useTls = useTls;
    return this;
  }

  @Override
  public ClientOptions setUseClientAuth(final boolean useClientAuth) {
    this.useClientAuth = useClientAuth;
    return this;
  }

  @Override
  public ClientOptions setTrustStore(final String trustStorePath) {
    this.trustStorePath = Objects.requireNonNull(trustStorePath);
    return this;
  }

  @Override
  public ClientOptions setTrustStorePassword(final String trustStorePassword) {
    this.trustStorePassword = Objects.requireNonNull(trustStorePassword);
    return this;
  }

  @Override
  public ClientOptions setKeyStore(final String keyStorePath) {
    this.keyStorePath = Objects.requireNonNull(keyStorePath);
    return this;
  }

  @Override
  public ClientOptions setKeyStorePassword(final String keyStorePassword) {
    this.keyStorePassword = Objects.requireNonNull(keyStorePassword);
    return this;
  }

  @Override
  public ClientOptions setBasicAuthCredentials(final String username, final String password) {
    Objects.requireNonNull(username);
    Objects.requireNonNull(password);

    this.useBasicAuth = true;
    this.basicAuthUsername = username;
    this.basicAuthPassword = password;
    return this;
  }

  @Override
  public ClientOptions unsetBasicAuthCredentials() {
    this.useBasicAuth = false;
    this.basicAuthUsername = "";
    this.basicAuthPassword = "";
    return this;
  }

  @Override
  public String getHost() {
    return host;
  }

  @Override
  public int getPort() {
    return port;
  }

  @Override
  public boolean isUseTls() {
    return useTls;
  }

  @Override
  public boolean isUseClientAuth() {
    return useClientAuth;
  }

  @Override
  public boolean isUseBasicAuth() {
    return useClientAuth;
  }

  @Override
  public String getTrustStore() {
    return trustStorePath;
  }

  @Override
  public String getTrustStorePassword() {
    return trustStorePassword;
  }

  @Override
  public String getKeyStore() {
    return keyStorePath;
  }

  @Override
  public String getKeyStorePassword() {
    return keyStorePassword;
  }

  @Override
  public String getBasicAuthUsername() {
    return basicAuthUsername;
  }

  @Override
  public String getBasicAuthPassword() {
    return basicAuthPassword;
  }

  @Override
  public ClientOptions copy() {
    return new ClientOptionsImpl(
        host, port,
        useTls, useClientAuth, useBasicAuth,
        trustStorePath, trustStorePassword,
        keyStorePath, keyStorePassword,
        basicAuthUsername, basicAuthPassword);
  }
}