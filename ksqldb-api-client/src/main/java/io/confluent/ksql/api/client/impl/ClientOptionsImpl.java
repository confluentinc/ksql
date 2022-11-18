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

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.api.client.ClientOptions;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class ClientOptionsImpl implements ClientOptions {

  private String host = ClientOptions.DEFAULT_HOST;
  private int port = ClientOptions.DEFAULT_HOST_PORT;
  private boolean useTls = false;
  private boolean verifyHost = true;
  private boolean useAlpn = false;
  private boolean useBasicAuth = false;
  private String trustStorePath;
  private String trustStorePassword;
  private String keyStorePath;
  private String keyStorePassword;
  private String keyPassword;
  private String keyAlias;
  private String basicAuthUsername;
  private String basicAuthPassword;
  private int executeQueryMaxResultRows = ClientOptions.DEFAULT_EXECUTE_QUERY_MAX_RESULT_ROWS;
  private int http2MultiplexingLimit = ClientOptions.DEFAULT_HTTP2_MULTIPLEXING_LIMIT;
  private Map<String, String> requestHeaders;

  /**
   * {@code ClientOptions} should be instantiated via {@link ClientOptions#create}, NOT via this
   * constructor.
   */
  public ClientOptionsImpl() {
  }

  // CHECKSTYLE_RULES.OFF: ParameterNumberCheck
  private ClientOptionsImpl(
      // CHECKSTYLE_RULES.ON: ParameterNumberCheck
      final String host, final int port,
      final boolean useTls, final boolean verifyHost, final boolean useAlpn,
      final boolean useBasicAuth,
      final String trustStorePath, final String trustStorePassword,
      final String keyStorePath, final String keyStorePassword, final String keyPassword,
      final String keyAlias, final String basicAuthUsername, final String basicAuthPassword,
      final int executeQueryMaxResultRows, final int http2MultiplexingLimit,
      final Map<String, String> requestHeaders) {
    this.host = Objects.requireNonNull(host);
    this.port = port;
    this.useTls = useTls;
    this.verifyHost = verifyHost;
    this.useAlpn = useAlpn;
    this.useBasicAuth = useBasicAuth;
    this.trustStorePath = trustStorePath;
    this.trustStorePassword = trustStorePassword;
    this.keyStorePath = keyStorePath;
    this.keyStorePassword = keyStorePassword;
    this.keyPassword = keyPassword;
    this.keyAlias = keyAlias;
    this.basicAuthUsername = basicAuthUsername;
    this.basicAuthPassword = basicAuthPassword;
    this.executeQueryMaxResultRows = executeQueryMaxResultRows;
    this.http2MultiplexingLimit = http2MultiplexingLimit;
    this.requestHeaders = requestHeaders;
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
  public ClientOptions setVerifyHost(final boolean verifyHost) {
    this.verifyHost = verifyHost;
    return this;
  }

  @Override
  public ClientOptions setUseAlpn(final boolean useAlpn) {
    this.useAlpn = useAlpn;
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
  public ClientOptions setKeyPassword(final String keyPassword) {
    this.keyPassword = keyPassword;
    return this;
  }

  @Override
  public ClientOptions setKeyAlias(final String keyAlias) {
    this.keyAlias = keyAlias;
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
  public ClientOptions setHttp2MultiplexingLimit(final int http2MultiplexingLimit) {
    this.http2MultiplexingLimit = http2MultiplexingLimit;
    return this;
  }

  @Override
  public ClientOptions setRequestHeaders(final Map<String, String> requestHeaders) {
    this.requestHeaders = requestHeaders == null ? null : ImmutableMap.copyOf(requestHeaders);
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
  public boolean isVerifyHost() {
    return verifyHost;
  }

  @Override
  public boolean isUseAlpn() {
    return useAlpn;
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
  public String getKeyPassword() {
    return keyPassword == null ? "" : keyPassword;
  }

  @Override
  public String getKeyAlias() {
    return keyAlias == null ? "" : keyAlias;
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
  public int getHttp2MultiplexingLimit() {
    return http2MultiplexingLimit;
  }

  @Override
  public Map<String, String> getRequestHeaders() {
    return requestHeaders == null ? new HashMap<>() : new HashMap<>(requestHeaders);
  }

  @Override
  public ClientOptions copy() {
    return new ClientOptionsImpl(
        host, port,
        useTls, verifyHost, useAlpn,
        useBasicAuth,
        trustStorePath, trustStorePassword,
        keyStorePath, keyStorePassword, keyPassword, keyAlias,
        basicAuthUsername, basicAuthPassword,
        executeQueryMaxResultRows, http2MultiplexingLimit,
        requestHeaders);
  }

  // CHECKSTYLE_RULES.OFF: CyclomaticComplexity
  @Override
  public boolean equals(final Object o) {
    // CHECKSTYLE_RULES.ON: CyclomaticComplexity
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final ClientOptionsImpl that = (ClientOptionsImpl) o;
    return port == that.port
        && useTls == that.useTls
        && verifyHost == that.verifyHost
        && useAlpn == that.useAlpn
        && executeQueryMaxResultRows == that.executeQueryMaxResultRows
        && host.equals(that.host)
        && Objects.equals(trustStorePath, that.trustStorePath)
        && Objects.equals(trustStorePassword, that.trustStorePassword)
        && Objects.equals(keyStorePath, that.keyStorePath)
        && Objects.equals(keyStorePassword, that.keyStorePassword)
        && Objects.equals(keyPassword, that.keyPassword)
        && Objects.equals(keyAlias, that.keyAlias)
        && Objects.equals(basicAuthUsername, that.basicAuthUsername)
        && Objects.equals(basicAuthPassword, that.basicAuthPassword)
        && http2MultiplexingLimit == that.http2MultiplexingLimit
        && Objects.equals(requestHeaders, that.requestHeaders);
  }

  @Override
  public int hashCode() {
    return Objects.hash(host, port, useTls, verifyHost, useAlpn, trustStorePath,
        trustStorePassword, keyStorePath, keyStorePassword, keyPassword, keyAlias,
        basicAuthUsername, basicAuthPassword, executeQueryMaxResultRows, http2MultiplexingLimit,
        requestHeaders);
  }

  @Override
  public String toString() {
    return "ClientOptions{"
        + "host='" + host + '\''
        + ", port=" + port
        + ", useTls=" + useTls
        + ", verifyHost=" + verifyHost
        + ", useAlpn=" + useAlpn
        + ", trustStorePath='" + trustStorePath + '\''
        + ", trustStorePassword='" + trustStorePassword + '\''
        + ", keyStorePath='" + keyStorePath + '\''
        + ", keyStorePassword='" + keyStorePassword + '\''
        + ", keyPassword='" + keyPassword + '\''
        + ", keyAlias='" + keyAlias + '\''
        + ", basicAuthUsername='" + basicAuthUsername + '\''
        + ", basicAuthPassword='" + basicAuthPassword + '\''
        + ", executeQueryMaxResultRows=" + executeQueryMaxResultRows
        + ", http2MultiplexingLimit=" + http2MultiplexingLimit
        + ", requestHeaders='" + requestHeaders + '\''
        + '}';
  }
}
