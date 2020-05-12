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
  private boolean verifyHost = true;
  private boolean trustAll = false;
  private boolean useBasicAuth = false;
  private String trustStorePath;
  private String trustStorePassword;
  private String keyStorePath;
  private String keyStorePassword;
  private String basicAuthUsername;
  private String basicAuthPassword;
  private int executeQueryMaxResultRows = 10000;

  public ClientOptionsImpl() {
  }

  // CHECKSTYLE_RULES.OFF: ParameterNumberCheck
  private ClientOptionsImpl(
      // CHECKSTYLE_RULES.ON: ParameterNumberCheck
      final String host, final int port,
      final boolean useTls, final boolean useClientAuth,
      final boolean verifyHost, final boolean trustAll,
      final boolean useBasicAuth,
      final String trustStorePath, final String trustStorePassword,
      final String keyStorePath, final String keyStorePassword,
      final String basicAuthUsername, final String basicAuthPassword,
      final int executeQueryMaxResultRows) {
    this.host = Objects.requireNonNull(host);
    this.port = port;
    this.useTls = useTls;
    this.useClientAuth = useClientAuth;
    this.verifyHost = verifyHost;
    this.trustAll = trustAll;
    this.useBasicAuth = useBasicAuth;
    this.trustStorePath = trustStorePath;
    this.trustStorePassword = trustStorePassword;
    this.keyStorePath = keyStorePath;
    this.keyStorePassword = keyStorePassword;
    this.basicAuthUsername = basicAuthUsername;
    this.basicAuthPassword = basicAuthPassword;
    this.executeQueryMaxResultRows = executeQueryMaxResultRows;
  }

  @Override
  public ClientOptions setHost(final String host) {
    this.host = host;
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
  public ClientOptions setVerifyHost(final boolean verifyHost) {
    this.verifyHost = verifyHost;
    return this;
  }

  @Override
  public ClientOptions setTrustStore(final String trustStorePath) {
    this.trustStorePath = trustStorePath;
    return this;
  }

  @Override
  public ClientOptions setTrustStorePassword(final String trustStorePassword) {
    this.trustStorePassword = trustStorePassword;
    return this;
  }

  @Override
  public ClientOptions setKeyStore(final String keyStorePath) {
    this.keyStorePath = keyStorePath;
    return this;
  }

  @Override
  public ClientOptions setKeyStorePassword(final String keyStorePassword) {
    this.keyStorePassword = keyStorePassword;
    return this;
  }

  @Override
  public ClientOptions setBasicAuthCredentials(final String username, final String password) {
    this.useBasicAuth = username != null || password != null;
    this.basicAuthUsername = username;
    this.basicAuthPassword = password;
    return this;
  }

  @Override
  public ClientOptions setExecuteQueryMaxResultRows(final int maxRows) {
    this.executeQueryMaxResultRows = maxRows;
    return this;
  }

  @Override
  public String getHost() {
    return host == null ? "" : host;
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
  public boolean isVerifyHost() {
    return verifyHost;
  }

  @Override
  public boolean isUseBasicAuth() {
    return useBasicAuth;
  }

  @Override
  public String getTrustStore() {
    return trustStorePath == null ? "" : trustStorePath;
  }

  @Override
  public String getTrustStorePassword() {
    return trustStorePassword == null ? "" : trustStorePassword;
  }

  @Override
  public String getKeyStore() {
    return keyStorePath == null ? "" : keyStorePath;
  }

  @Override
  public String getKeyStorePassword() {
    return keyStorePassword == null ? "" : keyStorePassword;
  }

  @Override
  public String getBasicAuthUsername() {
    return basicAuthUsername == null ? "" : basicAuthUsername;
  }

  @Override
  public String getBasicAuthPassword() {
    return basicAuthPassword == null ? "" : basicAuthPassword;
  }

  @Override
  public int getExecuteQueryMaxResultRows() {
    return executeQueryMaxResultRows;
  }

  @Override
  public ClientOptions copy() {
    return new ClientOptionsImpl(
        host, port,
        useTls, useClientAuth,
        verifyHost, trustAll,
        useBasicAuth,
        trustStorePath, trustStorePassword,
        keyStorePath, keyStorePassword,
        basicAuthUsername, basicAuthPassword,
        executeQueryMaxResultRows);
  }
}