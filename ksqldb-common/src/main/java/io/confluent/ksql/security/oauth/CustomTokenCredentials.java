/*
 * Copyright 2025 Confluent Inc.
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

public class CustomTokenCredentials implements Credentials {

  private Credentials customTokenCredentials;

  @Override
  public void configure(final Map<String, ?> configs) {
    final String className = (String) configs
            .get(KsqlClientConfig.CUSTOM_TOKEN_CREDENTIALS_CLASS);
    if (className == null || className.isEmpty()) {
      throw new ConfigException(String.format("Cannot configure CustomTokenCredentials "
              + "as %s is not configured", KsqlClientConfig.CUSTOM_TOKEN_CREDENTIALS_CLASS));
    }
    try {
      this.customTokenCredentials =
                (Credentials) Class.forName(className)
                        .getDeclaredConstructor()
                        .newInstance();
    } catch (Exception e) {
      throw new ConfigException(String.format(
                "Unable to instantiate an object of class %s, failed with exception: ",
              KsqlClientConfig.CUSTOM_TOKEN_CREDENTIALS_CLASS
        ) + e.getMessage());
    }
    this.customTokenCredentials.configure(configs);
  }

  @Override
  public String getAuthHeader() {
    return this.customTokenCredentials.getAuthHeader();
  }

}
