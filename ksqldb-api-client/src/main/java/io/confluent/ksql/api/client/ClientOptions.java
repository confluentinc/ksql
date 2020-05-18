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

public interface ClientOptions {

  ClientOptions setHost(String host);

  ClientOptions setPort(int port);

  ClientOptions setUseTls(boolean useTls);

  ClientOptions setVerifyHost(boolean verifyHost);

  ClientOptions setTrustStore(String trustStorePath);

  ClientOptions setTrustStorePassword(String trustStorePassword);

  ClientOptions setKeyStore(String keyStorePath);

  ClientOptions setKeyStorePassword(String keyStorePassword);

  ClientOptions setBasicAuthCredentials(String username, String password);

  ClientOptions setExecuteQueryMaxResultRows(int maxRows);

  String getHost();

  int getPort();

  boolean isUseTls();

  boolean isVerifyHost();

  boolean isUseBasicAuth();

  String getTrustStore();

  String getTrustStorePassword();

  String getKeyStore();

  String getKeyStorePassword();

  String getBasicAuthUsername();

  String getBasicAuthPassword();

  int getExecuteQueryMaxResultRows();

  ClientOptions copy();

  static ClientOptions create() {
    return new ClientOptionsImpl();
  }
}