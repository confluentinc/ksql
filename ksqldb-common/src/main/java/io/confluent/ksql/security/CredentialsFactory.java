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

package io.confluent.ksql.security;

import io.confluent.ksql.security.oauth.OAuthBearerCredentials;
import org.apache.kafka.common.config.ConfigException;
import io.confluent.ksql.security.oauth.StaticTokenCredentials;

@SuppressWarnings("checkstyle:HideUtilityClassConstructor")
public class CredentialsFactory {
  public static Credentials createCredentials(final AuthType authType,
                                              final String customTokenCredentialsClassName) {
    switch (authType) {
      case BASIC:
        return new BasicCredentials();
      case OAUTHBEARER:
        return new OAuthBearerCredentials();
      case CUSTOM:
        return getCustomCredentials(customTokenCredentialsClassName);
      case STATIC_TOKEN:
        return new StaticTokenCredentials();
      default:
        return null;
    }
  }

  private static Credentials getCustomCredentials(final String customTokenCredentialsClassName) {
    final Credentials customTokenCredentials;
    if (customTokenCredentialsClassName == null || customTokenCredentialsClassName.isEmpty()) {
      throw new ConfigException(String.format("Cannot configure CustomTokenCredentials "
              + "as %s is not configured", KsqlClientConfig.CUSTOM_TOKEN_CREDENTIALS_CLASS));
    }
    try {
      customTokenCredentials =
              (Credentials) Class.forName(customTokenCredentialsClassName)
                      .getDeclaredConstructor()
                      .newInstance();
    } catch (Exception e) {
      throw new ConfigException(String.format(
              "Unable to instantiate an object of class %s, failed with exception: ",
              KsqlClientConfig.CUSTOM_TOKEN_CREDENTIALS_CLASS
      ) + e.getMessage());
    }
    return customTokenCredentials;
  }
}
