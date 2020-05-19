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

package io.confluent.ksql.api.client;

import io.confluent.ksql.api.client.impl.ClientOptionsImpl;

/**
 * Options for the ksqlDB {@link Client}.
 */
public interface ClientOptions {

  String DEFAULT_HOST = "localhost";
  int DEFAULT_HOST_PORT = 8088;
  int DEFAULT_EXECUTE_QUERY_MAX_RESULT_ROWS = 10000;

  /**
   * Sets the host name of the ksqlDB server to connect to. Defaults to "localhost".
   *
   * @param host host name
   * @return a reference to this
   */
  ClientOptions setHost(String host);

  /**
   * Sets the host port of the ksqlDB server to connect to. Defaults to 8088.
   *
   * @param port host port
   * @return a reference to this
   */
  ClientOptions setPort(int port);

  /**
   * Sets whether TLS should be used when connecting to the ksqlDB server. Defaults to false.
   *
   * @param useTls whether TLS should be used
   * @return a reference to this
   */
  ClientOptions setUseTls(boolean useTls);

  /**
   * Sets whether hostname verification (for TLS) is enabled. Defaults to true.
   *
   * @param verifyHost whether hostname verification should be enabled
   * @return a reference to this
   */
  ClientOptions setVerifyHost(boolean verifyHost);

  /**
   * Sets whether ALPN should be used. Defaults to false.
   *
   * @param useAlpn whether ALPN should be used
   * @return a reference to this
   */
  ClientOptions setUseAlpn(boolean useAlpn);

  /**
   * Sets the trust store path.
   *
   * @param trustStorePath trust store path
   * @return a reference to this
   */
  ClientOptions setTrustStore(String trustStorePath);

  /**
   * Sets the trust store password.
   *
   * @param trustStorePassword trust store password
   * @return a reference to this
   */
  ClientOptions setTrustStorePassword(String trustStorePassword);

  /**
   * Sets the key store path.
   *
   * @param keyStorePath key store path
   * @return a reference to this
   */
  ClientOptions setKeyStore(String keyStorePath);

  /**
   * Sets the key store password.
   *
   * @param keyStorePassword key store password
   * @return a reference to this
   */
  ClientOptions setKeyStorePassword(String keyStorePassword);

  /**
   * Sets the username and password to be used for HTTP basic authentication when connecting to the
   * ksqlDB server. Basic authentication will be used unless both username and password are null
   * (the default).
   *
   * @param username username for basic authentication
   * @param password password for basic authentication
   * @return a reference to this
   */
  ClientOptions setBasicAuthCredentials(String username, String password);

  /**
   * Sets the maximum number of rows that may be returned in a {@link BatchedQueryResult}. Defaults
   * to 10000.
   *
   * @param maxRows number of rows
   * @return a reference to this
   */
  ClientOptions setExecuteQueryMaxResultRows(int maxRows);

  /**
   * Returns the host name of the ksqlDB server to connect to.
   *
   * @return host name
   */
  String getHost();

  /**
   * Returns the host port of the ksqlDB server to connect to.
   *
   * @return host port
   */
  int getPort();

  /**
   * Returns whether TLS should be used when connecting to the ksqlDB server.
   *
   * @return whether TLS should be used
   */
  boolean isUseTls();

  /**
   * Returns whether hostname verification (for TLS) is enabled.
   *
   * @return whether hostname verification is enabled
   */
  boolean isVerifyHost();

  /**
   * Returns whether ALPN should be used.
   *
   * @return whether ALPN should be used
   */
  boolean isUseAlpn();

  /**
   * Returns whether HTTP basic authentication will be used when connecting to the ksqlDB server.
   *
   * @return whether basic authentication will be used
   */
  boolean isUseBasicAuth();

  /**
   * Returns the trust store path.
   *
   * @return trust store path
   */
  String getTrustStore();

  /**
   * Returns the trust store password.
   *
   * @return trust store password
   */
  String getTrustStorePassword();

  /**
   * Returns the key store path.
   *
   * @return key store path
   */
  String getKeyStore();

  /**
   * Returns the key store password.
   *
   * @return key store password
   */
  String getKeyStorePassword();

  /**
   * Returns the username to be used for HTTP basic authentication, if applicable.
   *
   * @return username
   */
  String getBasicAuthUsername();

  /**
   * Returns the password to be used for HTTP basic authentication, if applicable.
   *
   * @return password
   */
  String getBasicAuthPassword();

  /**
   * Returns the maximum number of rows that may be returned in a {@link BatchedQueryResult}.
   *
   * @return number of rows
   */
  int getExecuteQueryMaxResultRows();

  /**
   * Creates a copy of these {@code ClientOptions}.
   *
   * @return the copy
   */
  ClientOptions copy();

  static ClientOptions create() {
    return new ClientOptionsImpl();
  }
}