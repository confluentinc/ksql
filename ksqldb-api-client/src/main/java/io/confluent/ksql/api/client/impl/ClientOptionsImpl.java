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
import io.confluent.ksql.api.client.exception.KsqlClientException;
import io.confluent.ksql.security.AuthType;
import io.confluent.ksql.security.oauth.IdpConfig;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class ClientOptionsImpl implements ClientOptions {

  private String host = ClientOptions.DEFAULT_HOST;
  private int port = ClientOptions.DEFAULT_HOST_PORT;
  private boolean useTls = false;
  private boolean verifyHost = true;
  private boolean useAlpn = false;
  private AuthType authType = AuthType.NONE;
  private String trustStorePath;
  private String trustStorePassword;
  private String keyStorePath;
  private String keyStorePassword;
  private String keyPassword;
  private String keyAlias;
  private String storeType;
  private String securityProviders;
  private String keyManagerAlgorithm;
  private String trustManagerAlgorithm;
  private String basicAuthUsername;
  private String basicAuthPassword;
  private int executeQueryMaxResultRows = ClientOptions.DEFAULT_EXECUTE_QUERY_MAX_RESULT_ROWS;
  private int http2MultiplexingLimit = ClientOptions.DEFAULT_HTTP2_MULTIPLEXING_LIMIT;
  private Map<String, String> requestHeaders;
  private IdpConfig idpConfig;

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
      final String trustStorePath, final String trustStorePassword,
      final String keyStorePath, final String keyStorePassword, final String keyPassword,
      final String keyAlias, final String storeType, final String securityProviders,
      final String keyManagerAlgorithm, final String trustManagerAlgorithm,
      final String basicAuthUsername, final String basicAuthPassword,
      final int executeQueryMaxResultRows, final int http2MultiplexingLimit,
      final Map<String, String> requestHeaders, final IdpConfig idpConfig,
      final AuthType authType) {
    this.host = Objects.requireNonNull(host);
    this.port = port;
    this.useTls = useTls;
    this.verifyHost = verifyHost;
    this.useAlpn = useAlpn;
    this.trustStorePath = trustStorePath;
    this.trustStorePassword = trustStorePassword;
    this.keyStorePath = keyStorePath;
    this.keyStorePassword = keyStorePassword;
    this.keyPassword = keyPassword;
    this.keyAlias = keyAlias;
    this.storeType = storeType;
    this.securityProviders = securityProviders;
    this.keyManagerAlgorithm = keyManagerAlgorithm;
    this.trustManagerAlgorithm = trustManagerAlgorithm;
    this.basicAuthUsername = basicAuthUsername;
    this.basicAuthPassword = basicAuthPassword;
    this.executeQueryMaxResultRows = executeQueryMaxResultRows;
    this.http2MultiplexingLimit = http2MultiplexingLimit;
    this.requestHeaders = requestHeaders;
    this.idpConfig = idpConfig;
    this.authType = authType;
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
  public ClientOptions setStoreType(final String storeType) {
    this.storeType = storeType;
    return this;
  }

  @Override
  public ClientOptions setSecurityProviders(final String securityProviders) {
    this.securityProviders = securityProviders;
    return this;
  }

  @Override
  public ClientOptions setKeyManagerAlgorithm(final String keyManagerAlgorithm) {
    this.keyManagerAlgorithm = keyManagerAlgorithm;
    return this;
  }

  @Override
  public ClientOptions setTrustManagerAlgorithm(final String trustManagerAlgorithm) {
    this.trustManagerAlgorithm = trustManagerAlgorithm;
    return this;
  }

  @Override
  public ClientOptions setBasicAuthCredentials(final String username, final String password) {
    if (authType == AuthType.OAUTHBEARER) {
      throw new KsqlClientException("Already configured with bearer auth. Cannot set basic auth.");
    }
    this.authType = AuthType.BASIC;
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
  public ClientOptions setIdpConfig(final IdpConfig idpConfig) {
    if (authType == AuthType.BASIC) {
      throw new KsqlClientException("Already configured with basic auth. Cannot set bearer auth.");
    }
    this.authType = AuthType.OAUTHBEARER;
    this.idpConfig = idpConfig;
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
  public AuthType getAuthType() {
    return authType;
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
  public String getStoreType() {
    return storeType == null ? "JKS" : storeType;
  }

  @Override
  public String getSecurityProviders() {
    return securityProviders == null ? "" : securityProviders;
  }

  @Override
  public String getKeyManagerAlgorithm() {
    return keyManagerAlgorithm == null ? "" : keyManagerAlgorithm;
  }

  @Override
  public String getTrustManagerAlgorithm() {
    return trustManagerAlgorithm == null ? "" : trustManagerAlgorithm;
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
  public IdpConfig getIdpConfig() {
    return idpConfig;
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
        trustStorePath, trustStorePassword,
        keyStorePath, keyStorePassword, keyPassword, keyAlias, storeType,
        securityProviders, keyManagerAlgorithm, trustManagerAlgorithm,
        basicAuthUsername, basicAuthPassword,
        executeQueryMaxResultRows, http2MultiplexingLimit,
        requestHeaders, idpConfig.copy(), authType);
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
        && Objects.equals(storeType, that.storeType)
        && Objects.equals(securityProviders, that.securityProviders)
        && Objects.equals(keyManagerAlgorithm, that.keyManagerAlgorithm)
        && Objects.equals(trustManagerAlgorithm, that.trustManagerAlgorithm)
        && Objects.equals(basicAuthUsername, that.basicAuthUsername)
        && Objects.equals(basicAuthPassword, that.basicAuthPassword)
        && http2MultiplexingLimit == that.http2MultiplexingLimit
        && Objects.equals(requestHeaders, that.requestHeaders)
        && Objects.equals(idpConfig, that.idpConfig);
  }

  @Override
  public int hashCode() {
    return Objects.hash(host, port, useTls,
        verifyHost, useAlpn, trustStorePath,
        trustStorePassword, keyStorePath, keyStorePassword, keyPassword, keyAlias, storeType,
        securityProviders, keyManagerAlgorithm, trustManagerAlgorithm,
        basicAuthUsername, basicAuthPassword,
        executeQueryMaxResultRows, http2MultiplexingLimit,
        requestHeaders, idpConfig);
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
        + ", storeType='" + storeType + '\''
        + ", securityProviders='" + securityProviders + '\''
        + ", keyManagerAlgorithm='" + keyManagerAlgorithm + '\''
        + ", trustManagerAlgorithm='" + trustManagerAlgorithm + '\''
        + ", basicAuthUsername='" + basicAuthUsername + '\''
        + ", basicAuthPassword='" + basicAuthPassword + '\''
        + ", executeQueryMaxResultRows=" + executeQueryMaxResultRows
        + ", http2MultiplexingLimit=" + http2MultiplexingLimit
        + ", requestHeaders='" + requestHeaders + '\''
        + ", idpConfig='" + idpConfig + '\''
        + '}';
  }
}
