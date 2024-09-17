/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.security;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.kafka.common.config.ConfigException;

public final class BasicCredentials implements Credentials {

  private String username;
  private String password;

  @Override
  public void configure(final Map<String, ?> map) {
    validateConfigs(map);
    this.username = (String) map.get(KsqlClientConfig.KSQL_BASIC_AUTH_USERNAME);
    this.password = (String) map.get(KsqlClientConfig.KSQL_BASIC_AUTH_PASSWORD);
  }

  public static BasicCredentials of(final String username, final String password) {
    final BasicCredentials basicCredentials = new BasicCredentials();
    final Map<String, String> configs = new HashMap<>();
    configs.put(KsqlClientConfig.KSQL_BASIC_AUTH_USERNAME, username);
    configs.put(KsqlClientConfig.KSQL_BASIC_AUTH_PASSWORD, password);
    basicCredentials.configure(configs);
    return basicCredentials;
  }

  public String username() {
    return username;
  }

  public String password() {
    return password;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final BasicCredentials that = (BasicCredentials) o;
    return Objects.equals(username, that.username)
        && Objects.equals(password, that.password);
  }

  @Override
  public int hashCode() {
    return Objects.hash(username, password);
  }

  @Override
  public String getAuthHeader() {
    return "Basic " + Base64.getEncoder()
        .encodeToString((this.username + ":" + this.password).getBytes(StandardCharsets.UTF_8));
  }

  @Override
  public void validateConfigs(final Map<String, ?> configs) throws ConfigException {
    final String username = (String) configs.get(KsqlClientConfig.KSQL_BASIC_AUTH_USERNAME);
    final String password = (String) configs.get(KsqlClientConfig.KSQL_BASIC_AUTH_PASSWORD);

    if ((username == null || username.isEmpty()) || (password == null || password.isEmpty())) {
      throw new ConfigException("Cannot configure BasicCredential without "
          + "proper username or password");
    }
  }
}
