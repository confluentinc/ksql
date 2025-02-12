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
import java.util.Map;

/**
 * Options for the ksqlDB {@link Client}.
 */
public interface ClientOptions {

  String DEFAULT_HOST = "localhost";
  int DEFAULT_HOST_PORT = 8088;
  int DEFAULT_EXECUTE_QUERY_MAX_RESULT_ROWS = 10000;
  int DEFAULT_HTTP2_MULTIPLEXING_LIMIT = -1;

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
   * Sets the key password.
   *
   * @param keyPassword key password
   * @return a reference to this
   */
  ClientOptions setKeyPassword(String keyPassword);

  /**
   * Sets the key alias.
   *
   * @param keyAlias key alias
   * @return a reference to this
   */
  ClientOptions setKeyAlias(String keyAlias);

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
   * Sets the maximum number of requests per HTTP/2 connection. Defaults to -1.
   *
   * @param http2MultiplexingLimit number of requests
   * @return a reference to this
   */
  ClientOptions setHttp2MultiplexingLimit(int http2MultiplexingLimit);

  /**
   * Sets custom request headers to be sent with requests to the ksqlDB server.
   * These headers are in addition to any automatic headers such as the
   * authorization header.
   *
   * <p>If this method is called more than once, only the headers passed on
   * the last invocation will be used. To update existing custom headers,
   * use this method in combination with {@link ClientOptions#getRequestHeaders()}.
   *
   * <p>In case of overlap between these custom headers and automatic headers such
   * as the authorization header, these custom headers take precedence.
   *
   * @param requestHeaders custom request headers
   * @return a reference to this
   */
  ClientOptions setRequestHeaders(Map<String, String> requestHeaders);

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
   * Returns the key password.
   *
   * @return key password
   */
  String getKeyPassword();

  /**
   * Returns the key alias.
   *
   * @return key alias
   */
  String getKeyAlias();

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
   * Returns the maximum number of requests per HTTP/2 connection.
   *
   * @return number of requests
   */
  int getHttp2MultiplexingLimit();

  /**
   * Returns a copy of the custom request headers to be sent with ksqlDB requests.
   * If not set, then this method returns an empty map.
   *
   * @return custom request headers
   */
  Map<String, String> getRequestHeaders();

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
