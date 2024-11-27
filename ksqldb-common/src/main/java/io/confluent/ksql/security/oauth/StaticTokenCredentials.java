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

package io.confluent.ksql.security.oauth;

import io.confluent.ksql.security.Credentials;
import io.confluent.ksql.security.KsqlClientConfig;
import java.util.Map;
import org.apache.kafka.common.config.ConfigException;

public class StaticTokenCredentials implements Credentials {

  private String token;

  @Override
  public String getAuthHeader() {
    return "Bearer " + this.token;
  }

  @Override
  public void validateConfigs(final Map<String, ?> configs) throws ConfigException {
    final String token = (String) configs
        .get(KsqlClientConfig.BEARER_AUTH_TOKEN_CONFIG);
    if (token == null || token.isEmpty()) {
      throw new ConfigException("Cannot configure StaticTokenCredentials without "
          + "a proper token.");
    }
  }

  @Override
  public void configure(final Map<String, ?> configs) {
    validateConfigs(configs);
    this.token = (String) configs
        .get(KsqlClientConfig.BEARER_AUTH_TOKEN_CONFIG);
  }
}
